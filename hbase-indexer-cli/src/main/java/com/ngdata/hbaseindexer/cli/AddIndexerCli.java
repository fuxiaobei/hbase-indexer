/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.cli;

import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * CLI tool to add a new {@link IndexerDefinition}s to the {@link IndexerModel}.
 */
public class AddIndexerCli extends AddOrUpdateIndexerCli {

    public static void main(String[] args) throws Exception {
        new AddIndexerCli().run(args);
    }

    private AddIndexerCli() {
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();
        indexConfOption.required();
        return parser;
    }

    @Override
    protected String getCmdName() {
        return "add-indexer";
    }

    public void run(OptionSet options) throws Exception {
        super.run(options);

        IndexerDefinitionBuilder builder = buildIndexerDefinition(options, null);
        IndexerDefinition indexer = builder.build();

        model.addIndexer(indexer);

        System.out.println("Indexer added: " + indexer.getName());
    }
}