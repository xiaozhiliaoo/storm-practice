package trident.strategy;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by lili on 2017/6/17.
 */
public class StrategyTopology {
    public static StormTopology buildTopology() {
        TridentTopology tridentTopology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("sub"),
                4,
                new Values("java"),
                new Values("python"),
                new Values("php"),
                new Values("c++"),
                new Values("ruby")
        );
        spout.setCycle(true);
        Stream inputStream = tridentTopology.newStream("spout", spout);
        //随机分组
        inputStream.shuffle()
                //分区分组
                //.partition(new Fields("sub"))
                //全局分组
                //.global()
                //广播分组
                //.broadcast()
                .each(new Fields("sub"), new WriteFunction(), new Fields()).parallelismHint(4);
        return tridentTopology.build();
    }

    @SneakyThrows
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setMaxSpoutPending(20);
        if (args == null || args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("trident-strategy", config, buildTopology());
            Thread.sleep(50000);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(args[0], config, buildTopology());
        }
    }


}
