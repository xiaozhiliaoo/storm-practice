package wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import wordcount.bolt.WordCountBolt;
import wordcount.bolt.WordReportBolt;
import wordcount.bolt.WordSplitBolt;
import wordcount.spout.WordSpout;
import wordcount.utils.Utils;


/**
 * Created by lili on 2017/6/17.
 */
public class WordTopology {

    public static final String WORD_SPOUT_ID = "word-spout";
    public static final String SPLIT_BOLT_ID = "split-bolt";
    public static final String COUNT_BOLT_ID = "count-bolt";
    public static final String REPORT_BOLT_ID = "report-bolt";
    public static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) {
        //实例对象
        WordSpout spout = new WordSpout();
        WordSplitBolt splitBolt = new WordSplitBolt();
        WordCountBolt countBolt = new WordCountBolt();
        WordReportBolt reportBolt = new WordReportBolt();

        // 构建拓扑图
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(WORD_SPOUT_ID,spout);
        // WordSpout -> WordSplitBolt
        builder.setBolt(SPLIT_BOLT_ID,splitBolt,5).shuffleGrouping(WORD_SPOUT_ID);
        // WordSplitBolt -> WordCountBolt  随机分组肯定不行  一个节点被另外统计了
        builder.setBolt(COUNT_BOLT_ID,countBolt,5).fieldsGrouping(SPLIT_BOLT_ID,new Fields("word"));
        // WordCountBolt -> WordReportBolt  统计时候全局分组
        builder.setBolt(REPORT_BOLT_ID,reportBolt,10).globalGrouping(COUNT_BOLT_ID);

        //本地配置
        Config config = new Config();
        config.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        // 集群提交拓扑
        cluster.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        Utils.waitForSeconds(10);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
