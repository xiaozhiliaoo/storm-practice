package trident.wordcount;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Created by lili on 2017/6/17.
 */
public class SplitFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        String subjects = tridentTuple.getStringByField("subjects");
        //取出句子发送
        for (String sub : subjects.split(" ")) {
            tridentCollector.emit(new Values(sub));
        }

    }
}
