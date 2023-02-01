package com.yang.spark2x.demo.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.yang.spark2x.demo.security.LoginUtil;
import com.yang.spark2x.demo.util.LogUtil;
import com.yang.spark2x.demo.kafka.KafkaConsumerConfig;
import com.yang.spark2x.demo.kafka.KafkaProducerConfig;
import com.yang.spark2x.demo.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

/**
 * @author yangliangchuang 2022/11/20 17:12
 */
public class SparkDemo {

    private static final Logger log = LoggerFactory.getLogger(SparkDemo.class);

    public static void main(String[] args) throws Exception {

        String userPrincipal = "washout";
        String userKeyTabPath = "/opt/FIClient4Washout/user.keytab";
        String krb5ConfPath = "/opt/FIClient4Washout/krb5.conf";
//        String userKeyTabPath = System.getProperty("user.dir") + "/config/" + "user.keytab";
//        String krb5ConfPath = System.getProperty("user.dir") + "/config/" + "krb5.conf";
        Configuration hadoopConf = new Configuration();
        try {
            LoginUtil.login(userPrincipal, userKeyTabPath, krb5ConfPath, hadoopConf);
        } catch (IOException e) {
            log.error(LogUtil.exceptionLog(e.toString(), "Login error."));
        }

        System.out.println("创建sparkConf之前读取配置文件");
        FileUtil fileUtil = new FileUtil();
        String test = fileUtil.collectTest();
        System.out.println(test);

        // 创建SparkStreaming运行环境
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkStreamingDemo");
//        sparkConf.setMaster("local[3]");
        sparkConf.setExecutorEnv("esAddre", "34.8.8.122:24100,34.8.8.123:24100,34.8.8.126:24100");
        sparkConf.setExecutorEnv("springApplicationName", "SparkStreamingDemo");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.milliseconds(1000));


        /** (1)数据源为交警的以萨二次识别数据 **/
        final JavaInputDStream<ConsumerRecord<String, String>> jjStream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList("KK-DOUBLE-RECOG-YISA-SELF"),
                        KafkaConsumerConfig.getConsumerParams("spark-streaming-demo", "latest")
                ));

        jjStream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                ObjectMapper mapper = new ObjectMapper();
                mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
                Producer<String, String> producer = KafkaProducerConfig.getProducer();
                while (consumerRecords.hasNext()) {
                    //数据清洗
                    ConsumerRecord<String, String> next = consumerRecords.next();
                    System.out.println("kafka message: " + next);
                    System.out.println("创建sparkConf之后读取，创建之前的配置文件内容:" + test);
                }
            });

            // some time later, after outputs have completed
            ((CanCommitOffsets) jjStream.inputDStream()).commitAsync(offsetRanges, (map, e) -> {
                if (e != null) {
                    log.error(LogUtil.exceptionLog(e.toString(), "Commit offset failed."));
                }
            });
        });

        //开启SparkStreaming处理
        jssc.start();
        jssc.awaitTermination();
    }
}
