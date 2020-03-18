package drpc.drpc1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Created by lili on 2017/6/17.
 */
public class BasicDRPCTopology {

    public static class ExclaimBolt extends BaseBasicBolt{

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String input = tuple.getString(1);
            basicOutputCollector.emit(new Values(tuple.getValue(0),input+"!"));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("id","result"));
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(),3);
        Config config = new Config();
        if(args==null || args.length == 0){
            //本地
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("drpc-demo",config,builder.createLocalTopology(drpc));
            //DRPC数据来源可能是本地，掉远程storm服务器，不需要写spout
            for(String word:new String[]{"hello","world"}){
                System.out.println("Result for\""+word+"\": "+ drpc.execute("exclamation",word));
            }
            cluster.shutdown();
            drpc.shutdown();
        }else{
            //远程
            config.setNumWorkers(2);

            StormSubmitter .submitTopology(args[0],config,builder.createRemoteTopology());

        }
    }
}
