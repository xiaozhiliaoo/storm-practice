package kafka.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Created by lili on 2017/6/18.
 */
public class KafkaTopology {
    public static void main(String[] args) {

        ZkHosts zkHosts =  new ZkHosts("192.168.31.121:2181");
        //消费组id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,"test1","","id7");

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        kafkaConfig.forceFromStart = true;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("KafkaSpout",new KafkaSpout(kafkaConfig),1);

        builder.setBolt("SentenceBolt",new SentenceBolt(),1).globalGrouping("KafkaSpout");

        builder.setBolt("PrinterBolt",new PrinterBolt(),1).globalGrouping("SentenceBolt");

        LocalCluster cluster = new LocalCluster();

        Config config = new Config();

        cluster.submitTopology("KafkaTopology", config, builder.createTopology());

        try {
            System.out.println("waiting to consume from kafka");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Thread interruted exception "+e);
        }

        cluster.killTopology("KafkaTopology");

        cluster.shutdown();


    }
}
