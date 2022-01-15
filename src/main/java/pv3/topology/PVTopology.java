package pv3.topology;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import pv3.bolt.LogAnalysis;
import pv3.bolt.PageViewCounter;
import pv3.spout.LogReader;
import wordcount.utils.Utils;

/**
 * Created by lili on 2017/6/18.
 */
public class PVTopology {

    @SneakyThrows
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new LogReader());
        builder.setBolt("log-analysis", new LogAnalysis()).shuffleGrouping("log-reader");
        builder.setBolt("pagevie-counter", new PageViewCounter(), 2).shuffleGrouping("log-analysis");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();


        //提交拓扑图
        cluster.submitTopology("log-process-topology", conf, builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology("log-process-topology");
        cluster.shutdown();

    }
}
