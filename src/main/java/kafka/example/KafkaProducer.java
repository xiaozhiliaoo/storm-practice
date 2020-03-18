package kafka.example;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by lili on 2017/6/18.
 */
public class KafkaProducer {
    private static final String topic = "test1";

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("zookeeper.connect","192.168.31.121:2181,192.168.31.122:2181,192.168.31.123:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","192.168.31.121:9092");
        props.put("request.required.acks","1");
        ProducerConfig config = new ProducerConfig(props);
        Producer p = new Producer(config);
        for (int i = 0; i < 10; i++) {
            p.send(new KeyedMessage<Integer,String>(topic,"hello kafka"+i));
            System.out.println("send message: "+"hello kafka "+i);
            TimeUnit.SECONDS.sleep(1);
        }
        p.close();
    }
}
