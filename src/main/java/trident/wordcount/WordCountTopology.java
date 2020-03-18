package trident.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;

/**
 * Created by lili on 2017/6/17.
 */
public class WordCountTopology {
    private static StormTopology buildTopology() {
        TridentTopology tridentTopology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("subjects"),
                4,
                new Values("java java php ruby c++"),
                new Values("python python python java c++"),
                new Values("php php php java ruby"),
                new Values("c++ java php ruby c++"),
                new Values("ruby python python python php")
        );
        //发一遍就不发了
        spout.setCycle(false);
        Stream inputStream = tridentTopology.newStream("spout", spout);
        //数据源随机发
        inputStream.shuffle()
                // 接受数据源subjects  出去的sub
                .each(new Fields("subjects"),new SplitFunction(),new Fields("sub"))
                //词汇分组 结束数据源sub
                .groupBy(new Fields("sub"))
                //分组的值进行统计  统计完的值count
                .aggregate(new Count(), new Fields("count"))
//                .aggregate(new Sum(), new Fields("sum"))
                //输出  结束数据源sub count  没有输出
                .each(new Fields("sub","count"),new ResultFunction(),new Fields()).parallelismHint(1);
        return tridentTopology.build();

    }

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setMaxSpoutPending(20);
        if (args == null || args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", config, buildTopology());
            Thread.sleep(50000);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(args[0], config, buildTopology());
        }
    }


}
