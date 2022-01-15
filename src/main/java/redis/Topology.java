package redis;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lili on 2017/6/21.
 */
public class Topology {

    @SneakyThrows
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        List<String> zks = new ArrayList<>();
        zks.add("192.168.1");

        List<String> cFs = new ArrayList<>();
        cFs.add("persona1");
        cFs.add("company");

        builder.setSpout("spout", new SampleSpout(), 2);

        builder.setBolt("bolt", new StormRedisBolt("192.168.1.1", 6379)).shuffleGrouping("spout");

        Config config = new Config();

        config.setDebug(true);

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("StormRedisTopology", config, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        localCluster.killTopology("StormRedisTopology");

        localCluster.shutdown();
    }
}
