package bussiness.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bussiness.bolt.LogMergeBolt;
import bussiness.bolt.LogStatBolt;
import bussiness.bolt.LogWriterBolt;
import bussiness.spout.BSpout;
import bussiness.spout.VSpout;


/**
 * Created by lili on 2017/6/18.
 */
public class BussinessTopology {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("log-vspout",new VSpout(),1);
        builder.setSpout("log-bspout",new BSpout(),1);

        builder.setBolt("log-merge",new LogMergeBolt(),2)
                .fieldsGrouping("log-vspout","visit",new Fields("user"))
                .fieldsGrouping("log-bspout","business",new Fields("user"));

        builder.setBolt("log-stat",new LogStatBolt(),2)
                .fieldsGrouping("log-merge",new Fields("srcid"));

        builder.setBolt("log-writer",new LogWriterBolt(),1).shuffleGrouping("log-stat");


        Config conf = new Config();
        conf.setNumAckers(0);
        conf.setNumWorkers(7);

        LocalCluster cluster = new LocalCluster();
        //提交拓扑图
        cluster.submitTopology("srcId-pay", conf, builder.createTopology());
        //会轮询nextTuple()方法
        Thread.sleep(1000000);
        cluster.killTopology("srcId-pay");
        cluster.shutdown();

    }
}
