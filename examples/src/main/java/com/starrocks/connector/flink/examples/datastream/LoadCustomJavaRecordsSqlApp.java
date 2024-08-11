/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.flink.examples.datastream;

import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.row.sink.StarRocksSinkRowBuilder;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * This example will show how to load records to StarRocks table using Flink DataStream.
 * Each record is a user-defined java object {@link RowData} in Flink, and will be loaded
 * as a row of StarRocks table.
 */
public class LoadCustomJavaRecordsSqlApp {

    public static void main(String[] args) throws Exception {
        // To run the example, you should prepare in the following steps
        // 1. create a primary key table in your StarRocks cluster. The DDL is
        //  CREATE DATABASE `test`;
        //    CREATE TABLE `test`.`score_board`
        //    (
        //        `id` int(11) NOT NULL COMMENT "",
        //        `name` varchar(65533) NULL DEFAULT "" COMMENT "",
        //        `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
        //    )
        //    ENGINE=OLAP
        //    PRIMARY KEY(`id`)
        //    COMMENT "OLAP"
        //    DISTRIBUTED BY HASH(`id`)
        //    PROPERTIES(
        //        "replication_num" = "1"
        //    );
        //
        // 2. replace the connector options "jdbc-url" and "load-url" with your cluster configurations
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        String jdbcUrl = params.get("jdbcUrl", "jdbc:mysql://127.0.0.1:9030");
        String loadUrl = params.get("loadUrl", "127.0.0.1:8080");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate records which use RowData as the container.
        RowData[] records = new RowData[]{
                new RowData(1, "starrocks-rowdata", 100),
                new RowData(2, "flink-rowdata", 100),
        };
        DataStream<RowData> source = env.fromElements(records);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table scoreBoardTable = tableEnv.fromDataStream(source);
        tableEnv.createTemporaryView("scoreBoardTable", scoreBoardTable);

        String sqls = "CREATE TABLE `score_board` (\n" +
                "    `id` INT,\n" +
                "    `name` STRING,\n" +
                "    `score` INT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'starrocks',\n" +
                "    'jdbc-url' = '" + jdbcUrl + "',\n" +
                "    'load-url' = '" + loadUrl + "',\n" +
                "    'database-name' = 'test',\n" +
                "    'table-name' = 'score_board',\n" +
                "    'username' = 'root',\n" +
                "    'password' = ''\n" +
                ");\n" +
                "  INSERT INTO `score_board` select id,name,score from scoreBoardTable";
        for (String sql : sqls.split(";")) {
            tableEnv.executeSql(sql);
        }

    }

    /**
     * A simple POJO which includes three fields: id, name and core,
     * which match the schema of the StarRocks table `score_board`.
     */
    public static class RowData {
        public int id;
        public String name;
        public int score;

        public RowData() {
        }

        public RowData(int id, String name, int score) {
            this.id = id;
            this.name = name;
            this.score = score;
        }
    }


}