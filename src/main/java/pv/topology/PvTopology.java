package pv.topology;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import pv.bolt.LogStat;
import pv.bolt.LogWriter;
import pv.spout.LogReader;


/**
 * Created by lili on 2017/6/18.
 */
public class PvTopology {

    @SneakyThrows
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new LogReader(), 1);
        builder.setBolt("log-stat", new LogStat(), 2).fieldsGrouping("log-reader", new Fields("user"));
        builder.setBolt("log-writer", new LogWriter(), 1).shuffleGrouping("log-stat");
        Config config = new Config();
        //集群提交
//        StormSubmitter.submitTopology("log-topology",config,builder.createTopology());
        LocalCluster cluster = new LocalCluster();
        //提交拓扑图
        cluster.submitTopology("log-topology", config, builder.createTopology());
        //会轮询nextTuple()方法
        Thread.sleep(10000);
        cluster.killTopology("log-topology");
        cluster.shutdown();
    }
}
