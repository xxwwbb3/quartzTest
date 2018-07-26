package com.huawei.test.spark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class SparkstreamingOnDirectKafka {
    public static JavaStreamingContext createContext() throws Exception {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("SparkStreamingOnKafkaDirect");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));
        // jsc.checkpoint("/user/tenglq/checkpoint");

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "fonova-hadoop1.fx01:9092,fonova-hadoop2.fx01:9092");
        kafkaParams.put("auto.offset.reset", "smallest");
        Set<String> topics = new HashSet<String>();
        topics.add("tlqtest3");

        final Map<String, String> params = new HashMap<String, String>();
        params.put("driverClassName", "com.mysql.jdbc.Driver");
        params.put("url", "jdbc:mysql://192.168.6.131:3306/kafkaoffset");
        params.put("username", "wenbo");
        params.put("password", "xwbxjh");

        Map<TopicAndPartition, Long> offsets = new HashMap<TopicAndPartition, Long>();
        DataSource ds = DruidDataSourceFactory.createDataSource(params);
        Connection conn = ds.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT topic,partition,offset FROM kafka_offsets WHERE topic = 'tlqtest3'");
        while (rs.next()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(rs.getString(1), rs.getInt(2));
            offsets.put(topicAndPartition, rs.getLong(3));
        }

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

        JavaDStream<String> lines = null;

        if (offsets.isEmpty()) {
            JavaPairInputDStream<String, String> pairDstream = KafkaUtils.createDirectStream(jsc, String.class,
                    String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
            lines = pairDstream
                    .transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                        private static final long serialVersionUID = 1L;

                        public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                            OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                            offsetRanges.set(offsets);
                            return rdd;
                        }
                    }).flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                        private static final long serialVersionUID = 1L;

                        public Iterable<String> call(Tuple2<String, String> t) throws Exception {
                            return Arrays.asList(t._2);
                        }
                    });
        } else {
            JavaInputDStream<String> dstream = KafkaUtils.createDirectStream(jsc, String.class, String.class,
                    StringDecoder.class, StringDecoder.class, String.class, kafkaParams, offsets,
                    new Function<MessageAndMetadata<String, String>, String>() {

                        private static final long serialVersionUID = 1L;

                        public String call(MessageAndMetadata<String, String> v1) throws Exception {
                            return v1.message();
                        }
                    });
            lines = dstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
                private static final long serialVersionUID = 1L;

                public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(offsets);
                    return rdd;
                }
            });

        }

        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 1L;

            public void call(JavaRDD<String> rdd) throws Exception {
                // 操作rdd
                List<String> map = rdd.collect();
                String[] array = new String[map.size()];
                System.arraycopy(map.toArray(new String[map.size()]), 0, array, 0, map.size());
                List<String> l = Arrays.asList(array);
                Collections.sort(l);
                for (String value : l) {
                    System.out.println(value);
                }

                // 保存offset
                DataSource ds = DruidDataSourceFactory.createDataSource(params);
                Connection conn = ds.getConnection();
                Statement stmt = conn.createStatement();
                for (OffsetRange offsetRange : offsetRanges.get()) {
                    ResultSet rs = stmt.executeQuery("select count(1) from kafka_offsets where topic='"
                            + offsetRange.topic() + "' and partition='" + offsetRange.partition() + "'");
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        if (count > 0) {
                            stmt.executeUpdate("update kafka_offsets set offset ='" + offsetRange.untilOffset()
                                    + "'  where topic='" + offsetRange.topic() + "' and partition='"
                                    + offsetRange.partition() + "'");
                        } else {
                            stmt.execute("insert into kafka_offsets(topic,partition,offset) values('"
                                    + offsetRange.topic() + "','" + offsetRange.partition() + "','"
                                    + offsetRange.untilOffset() + "')");
                        }
                    }

                    rs.close();
                }

                stmt.close();
                conn.close();
            }

        });

        return jsc;
    }

    public static void main(String[] args) {
        JavaStreamingContextFactory factory = new JavaStreamingContextFactory() {
            public JavaStreamingContext create() {
                try {
                    return createContext();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        // JavaStreamingContext jsc =
        // JavaStreamingContext.getOrCreate("/user/tenglq/checkpoint", factory);

        JavaStreamingContext jsc = factory.create();

        jsc.start();

        jsc.awaitTermination();
        jsc.close();

    }
}

