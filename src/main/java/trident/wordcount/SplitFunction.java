package trident.wordcount;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by lili on 2017/6/17.
 */
public class SplitFunction extends BaseFunction {

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {

        String subjects = tridentTuple.getStringByField("subjects");
        //取出句子发送
        for(String sub:subjects.split(" ")){
            tridentCollector.emit(new Values(sub));
        }

    }
}
