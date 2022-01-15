package drpc.drpc1;

import lombok.SneakyThrows;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by lili on 2017/6/17.
 */
public class BasicDRPCTopology {

    public static class ExclaimBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String input = tuple.getString(1);
            basicOutputCollector.emit(new Values(tuple.getValue(0), input + "!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id", "result"));
        }
    }

    @SneakyThrows
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);
        Config config = new Config();
        if (args == null || args.length == 0) {
            //本地
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("drpc-demo", config, builder.createLocalTopology(drpc));
            //DRPC数据来源可能是本地，掉远程storm服务器，不需要写spout
            for (String word : new String[]{"hello", "world"}) {
                System.out.println("Result for\"" + word + "\": " + drpc.execute("exclamation", word));
            }
            cluster.shutdown();
            drpc.shutdown();
        } else {
            //远程
            config.setNumWorkers(2);

            StormSubmitter.submitTopology(args[0], config, builder.createRemoteTopology());

        }
    }
}
