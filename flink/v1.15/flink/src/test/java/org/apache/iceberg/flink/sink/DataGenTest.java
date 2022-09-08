/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.sink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Test;

public class DataGenTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //生成数据
        String dataGenStr = " CREATE TABLE source_table (" +
                " id INT," +
                " score INT," +
                " address STRING," +
                " ts AS localtimestamp," +
                " WATERMARK FOR ts AS ts" +
                " ) WITH (" +
                "'connector' = 'datagen'," +
                "'rows-per-second'='5'," +
                "'fields.id.kind'='sequence'," +
                "'fields.id.start'='1'," +
                "'fields.id.end'='100'," +
                "'fields.score.min'='1'," +
                "'fields.score.max'='100'," +
                "'fields.address.length'='10'" +
                ")";

        //print table
        String print_table = " CREATE TABLE print_table (" +
                "         id INT," +
                "         score INT," +
                "        address STRING" +
                "        ) WITH (" +
                "          'connector' = 'print'" +
                "        )";

        tableEnv.executeSql(dataGenStr);
        tableEnv.executeSql(print_table);
        tableEnv.executeSql("insert into print_table select id,score,address from source_table");
    }

    @Test
    public void Flink2Iceberg(){
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://bigdata:9083',\n" +
                "  'clients'='2',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs://bigdata:9000/warehouse/path'\n" +
                ")");

        tenv.useCatalog("hive_catalog");
//        tenv.executeSql("CREATE DATABASE iceberg_db");
        tenv.useDatabase("iceberg_db");

        tenv.executeSql("drop table if exists sourceTable");
        tenv.executeSql("CREATE TABLE sourceTable (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='100',\n" +
                " 'fields.userid.kind'='random',\n" +
                " 'fields.userid.min'='1',\n" +
                " 'fields.userid.max'='100',\n" +
                "'fields.f_random_str.length'='10'\n" +
                ")");

        tenv.executeSql(
                "insert into hive_catalog.iceberg_db.test10 select * from hive_catalog.iceberg_db.sourceTable");

    }

}
