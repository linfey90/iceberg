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

import com.google.common.collect.Maps;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class IcebergHiveTest {
    // local setting
    private static final String HMS_URI = "thrift://bigdata:9083";
    private static final String HDFS_WARE = "hdfs://bigdata:9000/user/hive/warehouse";
    //    private static final String HMS_URI = "thrift://node14:9083";
//    private static final String HDFS_WARE = "hdfs://node14:9009/user/hive/warehouse";

    private StreamExecutionEnvironment env;
    private String catalogName = "hive_catalog";
    private HiveCatalog catalog;
    private Configuration conf;

    @Before
    public void before(){
        // 设置执行HDFS操作的用户，防止权限不够
        System.setProperty("HADOOP_USER_NAME", "linfey");

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        // 启动一个webUI，指定本地WEB-UI端口号
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000);

        conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("dfs.replication", "1");
        conf.set("hive.metastore.uris", HMS_URI);
        conf.set("hive.metastore.warehouse.dir", HDFS_WARE);
        conf.set("hive.metastore.schema.verification", "false");
        conf.set("datanucleus.schema.autoCreateTables", "true");

        catalog = new HiveCatalog();
        catalog.setConf(conf);
//        KerberosUtils.authKerberos(conf);
        catalog.initialize(catalogName, new HashMap<String, String>());
    }

    @Test
    public void tableShow(){
        //  获取运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                1, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //  创建HiveCatalog
        //  名称 - 库名 - 配置信息路径
//        HiveCatalog hiveCatalog = new HiveCatalog("myclusterHive", "default", "input");

        CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, conf, new HashMap<>());
        FlinkCatalog flinkCatalog=new FlinkCatalog(catalogName,"iceberg_db", Namespace.empty(),catalogLoader,false);
//        flinkCatalog.open();
        //  注册Catalog
        tableEnvironment.registerCatalog(catalogName,flinkCatalog);

        //  使用Catalog
        tableEnvironment.useCatalog(catalogName);

        //  执行SQL，操作Hive数据
        tableEnvironment.executeSql("show tables")
                .print();
//        tableEnvironment.executeSql("select * from test10")
//                .print();
        tableEnvironment.executeSql("insert into test10 values(1,'aaa')");

    }
    @Test
    public void testSqlExecute(){
        String tblName="test_iceberg_table_15";
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
//                .useBlinkPlanner() // 使用BlinkPlanner
                .inBatchMode() // Batch模式，默认为StreamingMode
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);
        CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, conf, new HashMap<>());
//        FlinkCatalog flinkCatalog=new FlinkCatalog(catalogName,"iceberg_db", Namespace.empty(),catalogLoader,false);
//        flinkCatalog.open();
//        tableEnvironment.registerCatalog(catalogName,flinkCatalog);
//        tableEnvironment.useCatalog(catalogName);
        tableEnvironment.executeSql("CREATE CATALOG hive_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive',\n" +
                "  'uri'='thrift://bigdata:9083',\n" +
                "  'clients'='2',\n" +
                "  'property-version'='1',\n" +
                "  'warehouse'='hdfs://bigdata:9000/warehouse/path'\n" +
                ");");
        tableEnvironment.executeSql("USE CATALOG hive_catalog");
        tableEnvironment.executeSql("USE  iceberg_db");
        tableEnvironment.executeSql(String.format("drop table if exists %s",tblName));
        String sql= String.format("CREATE TABLE %s( id BIGINT COMMENT 'unique id', data STRING)", tblName);
        tableEnvironment.executeSql(sql);
        sql= String.format("insert into %s values(1,'aaa')", tblName);
        tableEnvironment.executeSql(sql);
    }

    @Test
    public void datagen() throws Exception{
        String dbName = "iceberg_db";
        String tableName = "test10";

        boolean isTableExist = catalog.tableExists(TableIdentifier.of(dbName, tableName));
        System.out.println("isTableExist :" + isTableExist);
        if(!isTableExist){
            List<Types.NestedField> columns = new ArrayList<>();

            columns.add(Types.NestedField.required(1, "id", Types.IntegerType.get()));
            columns.add(Types.NestedField.required(2, "name", Types.StringType.get()));

            // 指定 pk 键  b 列
            Set<Integer> identifierId = new HashSet<>();
            identifierId.add(1);
            Map<String, Integer> identifierMap = new HashMap<>();
            identifierMap.put("id", 1);

            Schema schema = new Schema(columns, identifierMap);

            //指定分区键  a 列
//            PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
//                    .identity("wafer_start_date")
//                    .identity("step_id").
//                    build();

            //hiveCatalog.createNamespace(Namespace.of(dbName));
            Map<String, String> tableProperties = Maps.newHashMap();
//            tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true"); // engine.hive.enabled=true
//            tableProperties.put(TableProperties.FORMAT_VERSION, "2"); //v2 版本才支持delete file
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
            catalog.createTable(TableIdentifier.of(dbName, tableName), schema, null,
                    tableProperties);
        }
        CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, conf, new HashMap<>());

        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, TableIdentifier.of(dbName, tableName));

        DataStream<RowData> input = env.addSource(new MySource());

        FlinkSink.forRowData(input)
                .writeParallelism(2)
                .tableLoader(tableLoader)
                .overwrite(false)
//                .distributionMode(DistributionMode.HASH)
                .append();

        env.execute("Iceberg Data Generator");

    }

    static class MySource extends RichSourceFunction<RowData> {

        private static final long serialVersionUID = 1L;
        boolean flag = true;

        @Override
        public void run(SourceContext<RowData> ctx) throws Exception {
            while (flag) {
                GenericRowData row = new GenericRowData(2);
                row.setField(0, new Random().nextInt(100));
                row.setField(1, StringData.fromString(UUID.randomUUID().toString()));
                ctx.collect(row);
//                System.out.println("write success!");
//                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
