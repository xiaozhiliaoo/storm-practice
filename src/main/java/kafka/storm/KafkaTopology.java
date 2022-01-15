package kafka.storm;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by lili on 2017/6/18.
 */
public class KafkaTopology {

    @SneakyThrows
    public static void main(String[] args) {

        ZkHosts zkHosts = new ZkHosts("192.168.31.121:2181");
        //消费组id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "test1", "", "id7");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);

        builder.setBolt("SentenceBolt", new SentenceBolt(), 1).globalGrouping("KafkaSpout");

        builder.setBolt("PrinterBolt", new PrinterBolt(), 1).globalGrouping("SentenceBolt");

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();

        cluster.submitTopology("KafkaTopology", config, builder.createTopology());

        try {
            System.out.println("waiting to consume from kafka");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Thread interruted exception " + e);
        }

        cluster.killTopology("KafkaTopology");

        cluster.shutdown();


    }
}
