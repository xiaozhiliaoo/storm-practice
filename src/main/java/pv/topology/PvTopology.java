package pv.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import pv.bolt.LogStat;
import pv.bolt.LogWriter;
import pv.spout.LogReader;


/**
 * Created by lili on 2017/6/18.
 */
public class PvTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader",new LogReader(),1);
        builder.setBolt("log-stat",new LogStat(),2).fieldsGrouping("log-reader",new Fields("user"));
        builder.setBolt("log-writer",new LogWriter(),1).shuffleGrouping("log-stat");
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
