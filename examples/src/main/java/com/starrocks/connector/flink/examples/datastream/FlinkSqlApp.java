package com.starrocks.connector.flink.examples.datastream;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Felix
 * @date 2024/6/04
 * 加购事实表
 */
public class FlinkSqlApp {
    public static void main(String[] args) {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.PORT, 11000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 指定表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30), Time.seconds(3)));

        String sqls = "CREATE TABLE `score_board` (\n" +
                "    `id` INT,\n" +
                "    `name` STRING,\n" +
                "    `score` INT,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'starrocks',\n" +
                "    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',\n" +
                "    'load-url' = '127.0.0.1:8080',\n" +
                "    'database-name' = 'test',\n" +
                "    'table-name' = 'score_board',\n" +
                "    'username' = 'root',\n" +
                "    'password' = ''\n" +
                ");\n" +
                "  INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100)";
        for (String sql : sqls.split(";")) {
            tableEnv.executeSql(sql);
        }

    }
}
