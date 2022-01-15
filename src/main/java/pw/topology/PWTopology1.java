package pw.topology;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import pw.bolt.PrintBolt;
import pw.bolt.WriteBolt;
import pw.spout.PWSpout;


public class PWTopology1 {

    public static void main(String[] args) throws Exception {
        //配置信息
        Config cfg = new Config();
        cfg.setNumWorkers(2);
        cfg.setDebug(true);

        //配置拓扑图
        TopologyBuilder builder = new TopologyBuilder();
        // 数据源
        builder.setSpout("spout", new PWSpout());
        // bolt   随机分组
        builder.setBolt("print-bolt", new PrintBolt()).shuffleGrouping("spout");
        builder.setBolt("write-bolt", new WriteBolt()).shuffleGrouping("print-bolt");


        //1 本地模式  不依赖于本地环境
//		LocalCluster cluster = new LocalCluster();
//		//提交拓扑图
//		cluster.submitTopology("top1", cfg, builder.createTopology());
//		//会轮询nextTuple()方法
//		Thread.sleep(10000);
//		cluster.killTopology("top1");
//		cluster.shutdown();

        //2 集群模式  不停止

        StormSubmitter.submitTopology("top1", cfg, builder.createTopology());

    }
}
