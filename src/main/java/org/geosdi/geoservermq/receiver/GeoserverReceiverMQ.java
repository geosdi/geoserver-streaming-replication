/**
 *
 */
package org.geosdi.geoservermq.receiver;

import java.util.logging.Logger;
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
import org.geosdi.geoservermq.sender.GeoserverSenderMQ;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.config.GeoServerPersisterManager;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.config.util.XStreamPersisterFactory;
import org.geoserver.platform.GeoServerResourceLoader;
import org.geotools.util.logging.Logging;

/**
 * @author Francesco Izzi - geoSDI
 * @email francesco.izzi@geosdi.org
 *
 */
public class GeoserverReceiverMQ implements MessageListener {

    static Logger LOGGER = Logging.getLogger("org.geoserver.config");
    public final String brokerURL;
    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    private GeoserverSenderMQ geoserverSender;
    GeoServerResourceLoader rl;
    XStreamPersisterFactory xpf = new XStreamPersisterFactory();
    Catalog catalog;
    XStreamPersister xp;
    GeoServerPersisterManager gsp;

    public GeoserverReceiverMQ(GeoServerResourceLoader rl, Catalog catalog, String brokerURL, GeoserverSenderMQ geoserverSender)
            throws JMSException {

        this.rl = rl;
        this.catalog = catalog;
        this.catalog.setResourceLoader(rl);
        this.brokerURL = brokerURL;
        this.geoserverSender = geoserverSender;

        factory = new ActiveMQConnectionFactory(brokerURL);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Topic destination = session.createTopic("GeoServer-Queue");
        consumer = session.createConsumer(destination);
        consumer.setMessageListener(this);

        xp = xpf.createXMLPersister();
        xp.setCatalog(catalog);
        
        this.gsp = new GeoServerPersisterManager(rl, xp);
    }

    public void onMessage(Message message) {
        try {

            LOGGER.info(this.geoserverSender.toString());
            if (message.getStringProperty("sender").equals(this.geoserverSender.toString())) {
                LOGGER.info("Nothing to do");
            } else {
                if (message.getStringProperty("type").equals("FeatureTypeInfo") && !this.geoserverSender.isWorking()) {
                    TextMessage txtMessage = (TextMessage) message;
                    FeatureTypeInfo ft = (FeatureTypeInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    LOGGER.info(txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            catalog.remove(ft);
                            //this.gsp.removeFeatureType(ft);
                        } else {
                            catalog.add(ft);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }
                    this.geoserverSender.setIsWorking(false);
                } else if (message.getStringProperty("type").equals("LayerInfo") && !this.geoserverSender.isWorking()) {
                    TextMessage txtMessage = (TextMessage) message;
                    LOGGER.info(txtMessage.getText());
                    LayerInfo ft = (LayerInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            catalog.remove(ft);
                            //this.gsp.removeLayer(ft);
                        } else {
                            catalog.add(ft);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }
                } else if (message.getStringProperty("type")
                        .equals("DataStoreInfo") && !this.geoserverSender.isWorking()) {
                    
                    xp.setEncryptPasswordFields(false);
                    TextMessage txtMessage = (TextMessage) message;
                    LOGGER.info(txtMessage.getText());
                    DataStoreInfo ds = (DataStoreInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            this.catalog.remove(ds);
                            //this.gsp.removeDataStore(ds);
                        } else {
                            catalog.add(ds);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }

                } else {
                    System.out.println("Invalid message received.");
                }
            }
        } catch (JMSException e) {
            System.out.println("Caught:" + e);
            e.printStackTrace();
        }

    }
}
