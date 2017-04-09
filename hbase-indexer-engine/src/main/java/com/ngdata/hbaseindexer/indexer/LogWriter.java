package com.ngdata.hbaseindexer.indexer;

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


import com.google.common.collect.ListMultimap;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/*********************************************new***************************************************************/
public class LogWriter {

    public static final Log LOG = LogFactory.getLog(LogWriter.class);

    String writerId;
    Map<String, String> config;
    FSDataOutputStream rightOut;
    FSDataOutputStream errorOut;
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
    SimpleDateFormat formatSS = new SimpleDateFormat("yyyyMMddHHmmss");
    String baseLogPath;
    volatile String currentDate;
    String rightFileName;
    String errorFileName;
    String totalRightFileName;
    String totalErrorFileName;
    String processName;
    String collectionName;
    String indexName;
    String threadName;
    String tableName;
    IndexerConf conf;
    FileSystem fs;
    ReentrantLock lock;
    boolean init;
    boolean enableRightLog = false; //default disabled
    boolean enableErrorLog = true; //default enabled
    int flushInterval = 30 * 1000;
    StringBuffer sb = new StringBuffer();
    String[] logFields;

    public LogWriter(IndexerConf conf, String indexName, String tableName, String writerId) {
        this.indexName = indexName;
        this.tableName = tableName;
        this.writerId = writerId;
        this.conf = conf;
        this.collectionName = conf.getConnectionParams().get(SolrConnectionParams.COLLECTION);
    }

    public String getWriterId() {
        return writerId;
    }

    public synchronized void setConfig(Map<String, String> config) {
        if(init) return;
        this.config = config;
        this.baseLogPath = config.get("baseLogPath");
        if(StringUtils.isEmpty(baseLogPath)) {
            baseLogPath = "hbase_indexer/logs";
        }
        if(config.containsKey("logFlushInterval")) {
            this.flushInterval = Integer.parseInt(config.get("logFlushInterval"));
        }
        if(config.containsKey("enableErrorLog")) {
            this.enableErrorLog = Boolean.parseBoolean(config.get("enableErrorLog"));
        }
        if(config.containsKey("enableRightLog")) {
            this.enableRightLog = Boolean.parseBoolean(config.get("enableRightLog"));
        }
        if(config.containsKey("logFields")) {
            this.logFields = config.get("logFields").split(",");
        }

        processName = ManagementFactory.getRuntimeMXBean().getName();
        threadName = Thread.currentThread().getName();
        initFileNames();
        try {
            fs = FileSystem.get(conf.getHbaseConf());
        } catch (IOException e) {
            throw new RuntimeException("get fileSystem exception", e);
        }
        lock = new ReentrantLock();
        new FlushWork().start();
        new RollWork().start();
        init = true;
    }

    public void initFileNames() {
        currentDate = now();
        rightFileName = baseLogPath + "/" + currentDate + "/" + indexName + "/right/" + processName + "_" + threadName;
        errorFileName = baseLogPath + "/" + currentDate + "/" + indexName + "/error/" + processName + "_" + threadName;
        totalRightFileName = rightFileName + ".total";
        totalErrorFileName = errorFileName + ".total";
    }


    public void initRightOutput() throws IOException {
        if(rightOut == null) {
            Path p = new Path(rightFileName);
            if(!fs.exists(p)) {
                rightOut = fs.create(p, false);
            }
        }
    }

    public void initErrorOutput() throws IOException {
        if(errorOut == null) {
            Path p = new Path(errorFileName);
            if(!fs.exists(p)) {
                errorOut = fs.create(p, false);
            }
        }
    }


    public String now() {
        return format.format(new Date());
    }

    public String current() {
        return formatSS.format(new Date());
    }


    public boolean needRollLog() {
        return !currentDate.equals(now());
    }

    public void rollLog() throws IOException {
        try {
            if (rightOut != null) {
                try {
                    rightOut.close();
                } catch (IOException e) {
                    LOG.error("close rightOut exception", e);
                }
                rightOut = null;
            }

            if (errorOut != null) {
                try {
                    errorOut.close();
                } catch (IOException e) {
                    LOG.error("close rightOut exception", e);
                }

                errorOut = null;
            }
        } finally {
        }
        initFileNames();
    }


    public void logRightRecord(Collection<SolrInputDocument> inputDocuments) throws IOException {
        if(!enableRightLog) return;
        lock.lock();
        try {
            initRightOutput();
            write(rightOut, inputDocuments);
        } finally {
            lock.unlock();
        }
    }

    public void logErrorRecord(SolrInputDocument doc) throws IOException {
        if(!enableErrorLog) return;
        lock.lock();
        try {
            initErrorOutput();
            write(errorOut, doc);
        } finally {
            lock.unlock();
        }
    }

    public void write(FSDataOutputStream out, Collection<SolrInputDocument> inputDocuments) throws IOException {
        for(SolrInputDocument doc : inputDocuments) {
            write(out, doc);
        }
    }

    public void write(FSDataOutputStream out, SolrInputDocument doc) throws IOException {
//        sb.setLength(0);
//        sb.append("time=").append(current()).append("***")
//                .append("collectionName=").append(indexName).append("***")
//                .append("rowKey=").append(doc.getFieldValue("id")).append("***")
//                .append("applicationNum=").append(doc.getFieldValue("full-applicationNum")).append("***")
//                .append("hbaseTableName=").append(tableName);
        sb.setLength(0);
        sb.append("time=").append(current()).append("***")
                .append("collectionName=").append(collectionName).append("***")
                .append("rowKey=").append(doc.getFieldValue("id")).append("***");
        if(logFields != null) {
            for(String field : logFields) {
                sb.append(field).append("=").append(doc.getFieldValue(field)).append("***");
            }
        }
        sb.append("hbaseTableName=").append(tableName);
        out.write((sb.toString() + "\n").getBytes());
    }



    public void logErrorRecord(Result result, ListMultimap<String, Object> fields) throws IOException {
        if(!enableErrorLog) return;
        lock.lock();
        try {
            initErrorOutput();
            errorOut.write((getString(result, fields) + "\n").getBytes());
        } finally {
            lock.unlock();
        }
    }

    public String getString(Result result, ListMultimap<String, Object> fields) throws IOException {
        sb.setLength(0);
        sb.append("time=").append(current()).append("***")
                .append("collectionName=").append(collectionName).append("***")
                .append("rowKey=").append(new String(result.getRow())).append("***");
        if(logFields != null) {
            for(String field : logFields) {
                List list = fields.get(field);
                if(list != null && list.size() > 0) {
                    sb.append(field).append("=").append(list.get(0)).append("***");
                }

            }
        }
        sb.append("hbaseTableName=").append(tableName);
        return sb.toString();
    }


    class FlushWork extends Thread {

        long lastTouch = System.currentTimeMillis();

        public void run() {
            while(true) {
                long now = System.currentTimeMillis();
                if (now - lastTouch < flushInterval) {
                    synchronized (this) {
                        try {
                            this.wait(flushInterval - (now - lastTouch));
                        } catch (InterruptedException e) {
                            LOG.error("", e);
                        }
                    }
                } else {
                    lock.lock();
                    try {
                        lastTouch = now;
                        if (errorOut != null)
                            errorOut.hflush();
                        if (rightOut != null)
                            rightOut.hflush();
                    } catch (IOException e) {
                        LOG.error("hsync exception", e);
                    } finally {
                        lock.unlock();
                    }
                }
            }
        }
    }


    class RollWork extends Thread {

        int waitTime = 5000;

        public RollWork() {
        }

        public void run() {
            while(true) {
                if(needRollLog()) {
                    try {
                        lock.lock();
                        rollLog();
                    } catch (IOException e) {
                        LOG.error("roll log exception", e);
                        synchronized (this) {
                            try {
                                this.wait(waitTime);
                            } catch (InterruptedException ex) {
                                LOG.error("", ex);
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                } else {
                    synchronized (this) {
                        try {
                            this.wait(waitTime);
                        } catch (InterruptedException e) {
                            LOG.error("", e);
                        }
                    }
                }
            }
        }
    }

}

