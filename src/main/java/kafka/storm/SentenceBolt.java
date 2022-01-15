package kafka.storm;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lili on 2017/6/18.
 */
public class SentenceBolt extends BaseBasicBolt {

    private List<String> words = new ArrayList<>();


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        if (StringUtils.isBlank(word)) {
            return;
        }
        System.out.println("receive word:" + word);

        words.add(word);

        if (word.endsWith(".")) {
            collector.emit(ImmutableList.of((Object) StringUtils.join(words, " ")));
            words.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
