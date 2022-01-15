package message.topology;

import lombok.SneakyThrows;
import message.bolt.SpliterBolt;
import message.bolt.WriterBolt;
import message.spout.MessageSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;


/**
 * Created by lili on 2017/6/17.
 */
public class MessageTopology {


    @SneakyThrows
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MessageSpout());
        // 随机分组
        builder.setBolt("split-bolt", new SpliterBolt()).shuffleGrouping("spout");
        builder.setBolt("write-bolt", new WriterBolt()).shuffleGrouping("split-bolt");

        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("message", config, builder.createTopology());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.killTopology("message");
        cluster.shutdown();
    }
}
