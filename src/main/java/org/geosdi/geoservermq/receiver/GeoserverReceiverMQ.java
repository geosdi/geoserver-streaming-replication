/**
 * 
 */
package org.geosdi.geoservermq.receiver;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.config.util.XStreamPersisterFactory;
import org.geoserver.platform.GeoServerResourceLoader;

/**
 * @author fizzi
 * 
 */
public class GeoserverReceiverMQ implements MessageListener {

	public static String brokerURL = "tcp://localhost:61616";

	private ConnectionFactory factory;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	GeoServerResourceLoader rl;
	XStreamPersisterFactory xpf = new XStreamPersisterFactory();
	Catalog catalog;
	XStreamPersister xp;

	public GeoserverReceiverMQ(GeoServerResourceLoader rl, Catalog catalog)
			throws JMSException {

		this.rl = rl;
		this.catalog = catalog;
		this.catalog.setResourceLoader(rl);

		factory = new ActiveMQConnectionFactory(brokerURL);
		connection = factory.createConnection();
		connection.start();
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic destination = session.createTopic("GeoServer-Queue");
		consumer = session.createConsumer(destination);
		consumer.setMessageListener(this);
	}

	public void onMessage(Message message) {
		try {
			if (message.getStringProperty("type").equals("FeatureTypeInfo")) {
				TextMessage txtMessage = (TextMessage) message;
				FeatureTypeInfo ft = (FeatureTypeInfo) xp.getXStream().fromXML(
						txtMessage.getText());
				catalog.add(ft);
			} else if (message.getStringProperty("type").equals("LayerInfo")) {
				TextMessage txtMessage = (TextMessage) message;
				LayerInfo ft = (LayerInfo) xp.getXStream().fromXML(
						txtMessage.getText());
				catalog.add(ft);
			} else if (message.getStringProperty("type")
					.equals("DataStoreInfo")) {
				TextMessage txtMessage = (TextMessage) message;
				DataStoreInfo ds = (DataStoreInfo) xp.getXStream().fromXML(
						txtMessage.getText());
				catalog.add(ds);
			} else {
				System.out.println("Invalid message received.");
			}
		} catch (JMSException e) {
			System.out.println("Caught:" + e);
			e.printStackTrace();
		}

	}

}
