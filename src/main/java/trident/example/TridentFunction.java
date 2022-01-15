package trident.example;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Created by lili on 2017/6/17.
 */
public class TridentFunction {


    @SneakyThrows
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        Config config = new Config();
        config.setNumWorkers(2);
        config.setMaxSpoutPending(20);
        if (args == null || args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("trident-function", config, buildTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(args[0], config, buildTopology());
        }
    }

    /**
     * 处理逻辑要继承BaseFunction
     */
    private static class SumFunction extends BaseFunction {

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            System.out.println("传进来的内容是：" + tridentTuple);
            int a = tridentTuple.getInteger(0);
            int b = tridentTuple.getInteger(1);
            int sum = a + b;
            tridentCollector.emit(new Values(sum));
        }
    }

    private static class Result extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            System.out.println("传进来的是:" + tridentTuple);
            Integer a = tridentTuple.getIntegerByField("a");
            Integer b = tridentTuple.getIntegerByField("b");
            Integer c = tridentTuple.getIntegerByField("c");
            Integer d = tridentTuple.getIntegerByField("d");
            System.out.println("a:" + a + "  b:" + b + "  c:" + c + " d:" + d);
            Integer sum = tridentTuple.getIntegerByField("sum");
            System.out.println("sum:" + sum);
        }
    }

    private static StormTopology buildTopology() {
        // 链式编程
        TridentTopology topology = new TridentTopology();
        FixedBatchSpout spout = new FixedBatchSpout(
                //数据源批量发送
                new Fields("a", "b", "c", "d"),
                4,
                new Values(1, 4, 7, 10),
                new Values(1, 1, 3, 11),
                new Values(2, 2, 7, 1),
                new Values(2, 5, 7, 2)
        );

        spout.setCycle(true);
        //Stream是数据源
        Stream inputStream = topology.newStream("spout", spout);

        inputStream.each(new Fields("a", "b", "c", "d"), new SumFunction(), new Fields("sum"))
                .each(new Fields("a", "b", "c", "d", "sum"), new Result(), new Fields());
        return topology.build();
    }

}