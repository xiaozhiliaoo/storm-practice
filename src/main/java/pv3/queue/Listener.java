package pv3.queue;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

/**
 * ��Ϣ����
 * @author Administrator
 * ������Ϣ������
 *
 */
public class Listener implements MessageListener{

	
	public void onMessage(Message message) {
		// TODO Auto-generated method stub
		try {
			System.out.println("�յ�����Ϣ��"+((TextMessage)message).getText());
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
