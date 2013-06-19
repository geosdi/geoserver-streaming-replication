/**
 *
 */
package org.geosdi.geoservermq.receiver;

import java.util.List;
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
import org.geoserver.catalog.CascadeDeleteVisitor;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.ResourceInfo;
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
                            LOGGER.info("Removing ... " + ft.getName());
                            List<LayerInfo> selection = catalog.getLayers(catalog.getResource(ft.getId(), ResourceInfo.class));
                            System.out.println("Removing...: " +selection);                    
                            CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(catalog);
                            ft.accept(visitor);
                            for (LayerInfo li : selection) {
                                 li.accept(visitor);
                            }
                        } else {
                            LOGGER.info("Adding ... " + ft.getId());
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
                    LayerInfo li = (LayerInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            LOGGER.info("Removing ... " + li.getName());
                            CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(catalog);
                            li.accept(visitor);
                            //this.gsp.removeLayer(ft);
                        } else {
                            LOGGER.info("Adding ... " + li.getName());
                            catalog.add(li);
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
                            LOGGER.info("Removing ... " + ds.getName());

                            CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(catalog);
                            ds.accept(visitor);
                            //this.gsp.removeDataStore(ds);
                        } else {
                            LOGGER.info("Adding ... " + ds.getName());
                            catalog.add(ds);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }

                } else if (message.getStringProperty("type")
                        .equals("CoverageStoreInfo") && !this.geoserverSender.isWorking()) {
                    
                    xp.setEncryptPasswordFields(false);
                    TextMessage txtMessage = (TextMessage) message;
                    LOGGER.info(txtMessage.getText());
                    CoverageStoreInfo cs = (CoverageStoreInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            LOGGER.info("Removing ... " + cs.getName());

                            CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(catalog);
                            cs.accept(visitor);
                            //this.gsp.removeDataStore(ds);
                        } else {
                            LOGGER.info("Adding ... " + cs.getName());
                            catalog.add(cs);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }
                    
                }  else if (message.getStringProperty("type")
                        .equals("CoverageInfo") && !this.geoserverSender.isWorking()) {
                    
                    xp.setEncryptPasswordFields(false);
                    TextMessage txtMessage = (TextMessage) message;
                    LOGGER.info(txtMessage.getText());
                    CoverageInfo ci = (CoverageInfo) xp.getXStream().fromXML(
                            txtMessage.getText());
                    this.geoserverSender.setIsWorking(true);
                    try {
                        if (message.getStringProperty("operation").equals("REMOVE")) {
                            LOGGER.info("Removing ... " + ci.getName());

                            List<LayerInfo> selection = catalog.getLayers(catalog.getResource(ci.getId(), ResourceInfo.class));
                            System.out.println("Removing...: " +selection);                    
                            CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(catalog);
                            ci.accept(visitor);
                            for (LayerInfo li : selection) {
                                 li.accept(visitor);
                            }
                            //this.gsp.removeDataStore(ds);
                        } else {
                            LOGGER.info("Adding ... " + ci.getName());
                            catalog.add(ci);
                        }
                    } catch (Exception e) {
                        System.out.println("Caught:" + e);
                        e.printStackTrace();
                    } finally {
                        this.geoserverSender.setIsWorking(false);
                    }
                    
                }
                else {
                    System.out.println("Invalid message received.");
                }
            } 
        } catch (JMSException e) {
            System.out.println("Caught:" + e);
            e.printStackTrace();
        }

    }
}
