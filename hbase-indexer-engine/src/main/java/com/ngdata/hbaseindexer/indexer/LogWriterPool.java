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

import com.ngdata.hbaseindexer.conf.IndexerConf;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/*********************************************new***************************************************************/
public class LogWriterPool {

    private static LogWriterPool pool;
    private static Object lock = new Object();
    private IndexerConf conf;
    private ConcurrentHashMap<String, LogWriter> map = new ConcurrentHashMap<String, LogWriter>();


    private LogWriterPool(IndexerConf conf) {
        this.conf = conf;
    }


    public static LogWriterPool getInstance(IndexerConf conf) {
        if(pool == null) {
            synchronized (lock) {
                if(pool == null) {
                    pool = new LogWriterPool(conf);
                }
            }
        }
        return pool;
    }

    public LogWriter get(String indexerName, String table) {
        String key = indexerName + "|" + table + "|" + Thread.currentThread().getName();
        LogWriter writer = map.get(key);
        if(writer == null) {
            writer = new LogWriter(conf, indexerName, table, key);
            writer.setConfig(conf.getGlobalParams());
            map.put(key, writer);
        }
        return writer;
    }

}
