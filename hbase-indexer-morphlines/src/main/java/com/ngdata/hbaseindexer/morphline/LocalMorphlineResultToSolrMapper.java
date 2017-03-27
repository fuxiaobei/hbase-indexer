/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.morphline;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;

/**
 * Performs Result to Solr mapping using morphlines.
 * <p>
 * This class is not thread-safe.
 */
final class LocalMorphlineResultToSolrMapper implements ResultToSolrMapper, Configurable {

    private HBaseMorphlineContext morphlineContext;
    private Command morphline;
    private String morphlineFileAndId;
    private final Map<String, String> forcedRecordFields = new HashMap();
    private final Collector collector = new Collector();
    private boolean isSafeMode = false; // safe but slow (debug-only)

    private Timer mappingTimer;
    private Meter numRecords;
    private Meter numFailedRecords;
    private Meter numExceptionRecords;

    private LogWriter logWriter;

//    private Config override;

    /**
     * Information to be used for constructing a Get to fetch data required for indexing.
     */
    private Map<byte[], NavigableSet<byte[]>> familyMap;

    private static final Logger LOG = LoggerFactory.getLogger(LocalMorphlineResultToSolrMapper.class);

    public LocalMorphlineResultToSolrMapper() {
        logWriter = new LogWriter();
        LOG.info("init logWriter");
    }

    @Override
    public void configure(Map<String, String> config) {
        Map<String, String> params = config;
        if (LOG.isTraceEnabled()) {
            LOG.trace("CWD is {}", new File(".").getAbsolutePath());
            LOG.trace("Configuration:\n{}", Joiner.on("\n").join(new TreeMap(params).entrySet()));
        }

        FaultTolerance faultTolerance = new FaultTolerance(
                getBooleanParameter(FaultTolerance.IS_PRODUCTION_MODE, false, params),
                getBooleanParameter(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false, params),
                getStringParameter(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES,
                        SolrServerException.class.getName(), params));

        String morphlineFile = params.get(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM);
        String morphlineId = params.get(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM);
        if (morphlineFile == null || morphlineFile.trim().length() == 0) {
            throw new MorphlineCompilationException("Missing parameter: "
                    + MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, null);
        }
        this.morphlineFileAndId = morphlineFile + "@" + morphlineId;

        // share metric registry across threads for better (aggregate) reporting
        MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(morphlineFileAndId);

        this.morphlineContext = (HBaseMorphlineContext)new HBaseMorphlineContext.Builder()
                .setExceptionHandler(faultTolerance)
                .setMetricRegistry(metricRegistry)
                .build();

        Map morphlineVariables = new HashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String variablePrefix = MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".";
            if (entry.getKey().startsWith(variablePrefix)) {
                morphlineVariables.put(entry.getKey().substring(variablePrefix.length()), entry.getValue());
            }
        }
        Config override = ConfigFactory.parseMap(morphlineVariables);

        this.morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext, collector,
                override);

        /*********************************************add new***************************************************************/
        logWriter.setConfig(params);
        try {
            Class c = Class.forName("org.kitesdk.morphline.stdlib.Pipe");
            Method method = c.getDeclaredMethod("getChild");
            method.setAccessible(true);

            Command childCommand = (Command) method.invoke(morphline);

            c = Class.forName("org.kitesdk.morphline.base.AbstractCommand");
            method = c.getDeclaredMethod("getConfig");
            method.setAccessible(true);

            Config conf = (Config) method.invoke(childCommand);
            collector.setConfig(conf);
        } catch (Throwable e) {
            throw new RuntimeException("collector set config exception", e);
        }
        /**********************************************end**************************************************************/

        for (Map.Entry<String,String> entry : params.entrySet()) {
            String fieldPrefix = MorphlineResultToSolrMapper.MORPHLINE_FIELD_PARAM + ".";
            if (entry.getKey().startsWith(fieldPrefix)) {
                forcedRecordFields.put(entry.getKey().substring(fieldPrefix.length()), entry.getValue());
            }
        }
        LOG.debug("Record fields passed by force to this morphline: {}", forcedRecordFields);

        // precompute familyMap; see DefaultResultToSolrMapper ctor
        Get get = new Get(Bytes.toBytes(" "));
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            byte[] columnFamily = extractor.getColumnFamily();
            byte[] columnQualifier = extractor.getColumnQualifier();
            if (columnFamily != null) {
                if (columnQualifier != null) {
                    if (get.getFamilyMap().containsKey(columnFamily) &&
                            get.getFamilyMap().get(columnFamily) == null) {
                        ; // do nothing to honour pre-existing request to fetch all columns from that family
                    } else {
                        get.addColumn(columnFamily, columnQualifier);
                    }
                } else {
                    get.addFamily(columnFamily);
                }
            }
        }
        this.familyMap = get.getFamilyMap();

        this.isSafeMode = getBooleanParameter("isSafeMode", false, params); // intentionally undocumented, not a public
        // API

        this.mappingTimer = morphlineContext.getMetricRegistry().timer(
                MetricRegistry.name("morphline.app", Metrics.ELAPSED_TIME));
        this.numRecords = morphlineContext.getMetricRegistry().meter(
                MetricRegistry.name("morphline.app", Metrics.NUM_RECORDS));
        this.numFailedRecords = morphlineContext.getMetricRegistry().meter(
                MetricRegistry.name("morphline.app", "numFailedRecords"));
        this.numExceptionRecords = morphlineContext.getMetricRegistry().meter(
                MetricRegistry.name("morphline.app", "numExceptionRecords"));

        Notifications.notifyBeginTransaction(morphline);
    }

    @Override
    public boolean containsRequiredData(Result result) {
        if (isSafeMode) {
            return false;
        }
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            if (!extractor.containsTarget(result)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        if (isSafeMode) {
            return true;
        }
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            if (extractor.isApplicable(kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Get getGet(byte[] row) {
        Get get = new Get(row);
        if (isSafeMode) {
            return get;
        }
        for (Entry<byte[], NavigableSet<byte[]>> familyMapEntry : familyMap.entrySet()) {
            // see DefaultResultToSolrMapper
            byte[] columnFamily = familyMapEntry.getKey();
            if (familyMapEntry.getValue() == null) {
                get.addFamily(columnFamily);
            } else {
                for (byte[] qualifier : familyMapEntry.getValue()) {
                    get.addColumn(columnFamily, qualifier);
                }
            }
        }
        return get;
    }

    @Override
    public void map(Result result, SolrUpdateWriter solrUpdateWriter) {
        numRecords.mark();
        Timer.Context timerContext = mappingTimer.time();
        try {
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, result);
            record.put(Fields.ATTACHMENT_MIME_TYPE, MorphlineResultToSolrMapper.OUTPUT_MIME_TYPE);
            for (Map.Entry<String, String> entry : forcedRecordFields.entrySet()) {
                record.replaceValues(entry.getKey(), entry.getValue());
            }
            collector.reset(solrUpdateWriter);
            try {
                Notifications.notifyStartSession(morphline);
                if (!morphline.process(record)) {
                    numFailedRecords.mark();
                    LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
                }
                /*********************************************modify***************************************************************/
                try {
                    logWriter.logRightRecord(result, record);

                } catch (IOException e) {
                    throw new MorphlineRuntimeException(e);
                }
                /*********************************************modify***************************************************************/

            } catch (RuntimeException t) {
                numExceptionRecords.mark();
                morphlineContext.getExceptionHandler().handleException(t, record);
                /*********************************************modify***************************************************************/
                try {
                    logWriter.logErrorRecord(result, record);

                } catch (IOException e) {
                    throw new MorphlineRuntimeException(e);
                }
                /*********************************************modify***************************************************************/
            }
        } finally {
            collector.reset(null);
            timerContext.stop();
        }
    }

    private boolean getBooleanParameter(String name, boolean defaultValue, Map<String, String> map) {
        String value = map.get(name);
        return value == null ? defaultValue : "TRUE".equalsIgnoreCase(value);
    }

    private String getStringParameter(String name, String defaultValue, Map<String, String> map) {
        String value = map.get(name);
        return value == null ? defaultValue : value;
    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private static final class Collector implements Command {

        private Map<String, String> fieldType = new HashMap<>();
        private Gson gson = new Gson();

        private SolrUpdateWriter solrUpdateWriter;

        public void setConfig(Config config) {
            List<? extends Config> list = config.getConfigList("mappings");
            for(Config c : list) {
                fieldType.put(c.getString("outputField"), c.getString("type"));
            }
        }

        public void reset(SolrUpdateWriter solrUpdateWriter) {
            this.solrUpdateWriter = solrUpdateWriter; // dubious lifecycle management 
            // this should better be passed in ResultToSolrMapper constructor or configure() method
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public boolean process(Record record) {
            Preconditions.checkNotNull(record);
            solrUpdateWriter.add(convert(record));
            return true;
        }

        private SolrInputDocument convert(Record record) {
            Map<String, Collection<Object>> map = record.getFields().asMap();
            SolrInputDocument doc = new SolrInputDocument(new HashMap(2 * map.size()));
            /*********************************************modify***************************************************************/
            doc.addField("content_type", "p");
            /*********************************************end***************************************************************/
            for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
                /*********************************************modify***************************************************************/
                if(fieldType.get(entry.getKey()).equals("com.ngdata.hbaseindexer.parse.JsonByteArrayValueMapper")
                        && entry.getValue().size() > 0) {
                    processNestedDoc(doc, entry.getKey(), (String) entry.getValue().iterator().next());
                } else {
                    doc.setField(entry.getKey(), entry.getValue());
                }
                /*********************************************end***************************************************************/
            }
            return doc;
        }


        /*********************************************add new***************************************************************/
        public void processNestedDoc(SolrInputDocument parent, String field, String json) {
            if(json == null)
                return;

            if(json.startsWith("{")) {
                Map<String, Object> jsonMap = gson.fromJson(new String(json), Map.class);
                parent.addChildDocument(processJsonMap(field, jsonMap));
            } else if(json.startsWith("[")) {
                List<Map<String, Object>> childs = gson.fromJson(new String(json), List.class);
                for(Map<String, Object> jsonMap : childs) {
                    parent.addChildDocument(processJsonMap(field, jsonMap));
                }
            } else {
                throw new RuntimeException("bad json format: " + json);
            }
        }


        public SolrInputDocument processJsonMap(String field, Map<String, Object> jsonMap) {
            SolrInputDocument doc = new SolrInputDocument(new HashMap(2 * jsonMap.size()));
            doc.addField("id", UUID.randomUUID().toString());
            for(Entry<String, Object> entry : jsonMap.entrySet()) {
                Object v = entry.getValue();
                if(v instanceof List) {
                    List mutliValues = (List) v;
                    for(Object fieldValue : mutliValues) {
                        doc.addField(field + "." + entry.getKey(), fieldValue);
                    }
                } else {
                    doc.addField(field + "." + entry.getKey(), entry.getValue());
                }
            }
            return doc;
        }
        /*********************************************end***************************************************************/
    }

}
