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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by wangkai8 on 17/4/6.
 */
public class LogWriterPool {

    private static LogWriterPool pool;
    private static Object lock = new Object();
    private Map<String, String> config;
    private ConcurrentHashMap<String, LogWriter> map = new ConcurrentHashMap<String, LogWriter>();


    private LogWriterPool(Map<String, String> config) {
        this.config = config;
    }


    public static LogWriterPool getInstance(Map<String, String> config) {
        if(pool == null) {
            synchronized (lock) {
                if(pool == null) {
                    pool = new LogWriterPool(config);
                }
            }
        }
        return pool;
    }

    public LogWriter get(String indexerName, String table) {
        String key = indexerName + "|" + table + "|" + Thread.currentThread().getName();
        LogWriter writer = map.get(key);
        if(writer == null) {
            writer = new LogWriter(indexerName, table, key);
            writer.setConfig(config);
            map.put(key, writer);
        }
        return writer;
    }

    public void returnPool(LogWriter writer) {
        map.put(writer.getWriterId(), writer);
    }

}
