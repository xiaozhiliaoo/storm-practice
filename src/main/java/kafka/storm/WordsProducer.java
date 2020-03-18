package kafka.storm;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by lili on 2017/6/18.
 */
public class WordsProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.31.121:2181,192.168.31.122:2181,192.168.31.123:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","192.168.31.121:9092");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String,String> producer = new Producer<String, String>(config);
        for(String word:METAMORPHOSIS_OPENING_PARA.split("\\s")){
            KeyedMessage<String,String> data = new KeyedMessage<String, String>("test1",word);
            producer.send(data);
        }
        System.out.println("producer data");
        producer.close();
    }

    private static String METAMORPHOSIS_OPENING_PARA = "ddd ddd sss kkkk lll ddd kkk , "+
            "he ff ss r qe gh xvb wss qaz bb "+
            "nj mos edc hhb qaa fg nvc rt 564 dd";
}
