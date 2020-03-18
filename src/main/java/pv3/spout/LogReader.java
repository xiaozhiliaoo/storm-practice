package pv3.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogReader implements IRichSpout{

    private static final String USERNAME = "lili"; // 默认的连接用户名
    private static final String PASSWORD = "lili"; // 默认的连接密码
    private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL; // 默认的连接地址
    private static final long serialVersionUID = 1L;
    private TopologyContext context;
    private SpoutOutputCollector collector;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer consumer;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.context = topologyContext;
        this.collector = spoutOutputCollector;
        this.connectionFactory = new ActiveMQConnectionFactory(
                LogReader.USERNAME,
                LogReader.PASSWORD,
                LogReader.BROKEURL);

        try {
            connection=connectionFactory.createConnection();  // 通过连接工厂获取连接
            connection.start(); // 启动连接
            session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE); // 创建Session 消费不用加事务
            destination=session.createQueue("LogQueue");  // 创建连接的消息队列
            consumer=session.createConsumer(destination); // 创建消息消费者
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        ////用recive方法接受消息，10000ms接收一次  开发不用recive
        try {
            TextMessage message = (TextMessage) consumer.receive(100000);
            System.out.println("发送的消息是：" + message.getText());
            this.collector.emit(new Values(message.getText()));
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logline"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
