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

import com.google.gson.Gson;
import com.ngdata.hbaseindexer.indexer.ThreadLocalCollection;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;
import org.kitesdk.morphline.api.Record;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;


public class LogWriter {

    public static final Log LOG = LogFactory.getLog(LogWriter.class);

    Map<String, String> config;
    FSDataOutputStream rightOut;
    FSDataOutputStream errorOut;
    SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHH");
    String baseLogPath;
    volatile String currentDate;
    String rightFileName;
    String errorFileName;
    String totalRightFileName;
    String totalErrorFileName;
    String processName;
    String indexName;
    String threadName;
    FileSystem fs;
    ReentrantLock lock;
    boolean init;
    boolean enableRightLog = false; //default disabled
    boolean enableErrorLog = true; //default enabled
    int flushInterval = 30 * 1000;
    Gson gson = new Gson();
    Map<String, String> record = new HashMap<String, String>();

    public LogWriter() {

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

        processName = ManagementFactory.getRuntimeMXBean().getName();
        indexName = ThreadLocalCollection.getCollection();
        threadName = Thread.currentThread().getName();
        initFileNames();
        try {
            fs = FileSystem.get(new Configuration());
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


    public void logRightRecord(Result result, Record record) throws IOException {
        if(!enableRightLog) return;
        lock.lock();
        try {
            initRightOutput();
            rightOut.write((getString(result) + "\n").getBytes());
        } finally {
            lock.unlock();
        }
    }


    public void logErrorRecord(Result result, Record record) throws IOException {
        if(!enableErrorLog) return;
        lock.lock();
        try {
            initErrorOutput();
            errorOut.write((getString(result) + "\n").getBytes());
        } finally {
            lock.unlock();
        }
    }

    public String getString(Result result) throws IOException {
        record.clear();
        CellScanner cs = result.cellScanner();
        while(cs.advance()) {
            Cell cell = cs.current();
            byte[] value = cell.getValueArray();
            record.put(new String(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                    value == null ? "" : new String(value, cell.getValueOffset(), cell.getValueLength()));
        }

        return gson.toJson(record);
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
