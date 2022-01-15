package pw.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import pw.bolt.PrintBolt;
import pw.bolt.WriteBolt;
import pw.spout.PWSpout;


public class PWTopology3 {

    public static void main(String[] args) throws Exception {

        Config cfg = new Config();
        cfg.setNumWorkers(2);
        cfg.setDebug(true);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new PWSpout(), 4);
        builder.setBolt("print-bolt", new PrintBolt(), 4).shuffleGrouping("spout");
        //随机分组
//		builder.setBolt("write-bolt",new WriteBolt(),8).shuffleGrouping("print-bolt");

        //设置字段分组  8个可能产生5个文件，存在线程争抢现象。同样的数据放在一个文件里， 但是不能同样数据保证放在一个文件中。
//		builder.setBolt("write-bolt", new WriteBolt(), 1).fieldsGrouping("print-bolt", new Fields("write"));
//		builder.setBolt("write-bolt", new WriteBolt(), 8).fieldsGrouping("print-bolt", new Fields("write"));
//		builder.setBolt("write-bolt", new WriteBolt(), 5).fieldsGrouping("print-bolt", new Fields("write"));

        //设置广播分组  生成7个文件  每个文件内容一样  但是顺序不一定一样。
//		builder.setBolt("write-bolt", new WriteBolt(), 7).allGrouping("print-bolt");

        //设置全局分组  4个bolt  一个bolt组件对应多个task  一对多  4个task  分配给id最低的
        builder.setBolt("write-bolt", new WriteBolt(), 4).globalGrouping("print-bolt");

        //1 本地模式
        cfg.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("top3", cfg, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("top3");
        cluster.shutdown();

        //2 集群模式
//		StormSubmitter.submitTopology("top3", cfg, builder.createTopology());

    }
}
