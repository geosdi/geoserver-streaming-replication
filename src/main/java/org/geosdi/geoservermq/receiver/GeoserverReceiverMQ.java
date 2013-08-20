/*
 *  geoserver-streaming-replication
 *  GeoServer Enterprise Extension for cluster architectures
 *  https://github.com/geosdi/geoserver-streaming-replication
 * ====================================================================
 *
 * Copyright (C) 2012-2013 geoSDI Group (CNR IMAA - Potenza - ITALY).
 *
 * This program is free software: you can redistribute it and/or modify it 
 * under the terms of the GNU General Public License as published by 
 * the Free Software Foundation, either version 3 of the License, or 
 * (at your option) any later version. This program is distributed in the 
 * hope that it will be useful, but WITHOUT ANY WARRANTY; without 
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR 
 * A PARTICULAR PURPOSE. See the GNU General Public License 
 * for more details. You should have received a copy of the GNU General 
 * Public License along with this program. If not, see http://www.gnu.org/licenses/ 
 *
 * ====================================================================
 *
 * Linking this library statically or dynamically with other modules is 
 * making a combined work based on this library. Thus, the terms and 
 * conditions of the GNU General Public License cover the whole combination. 
 * 
 * As a special exception, the copyright holders of this library give you permission 
 * to link this library with independent modules to produce an executable, regardless 
 * of the license terms of these independent modules, and to copy and distribute 
 * the resulting executable under terms of your choice, provided that you also meet, 
 * for each linked independent module, the terms and conditions of the license of 
 * that module. An independent module is a module which is not derived from or 
 * based on this library. If you modify this library, you may extend this exception 
 * to your version of the library, but you are not obligated to do so. If you do not 
 * wish to do so, delete this exception statement from your version. 
 *
 */
package org.geosdi.geoservermq.receiver;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.geosdi.geoservermq.sender.GeoserverSenderMQ;
import org.geoserver.catalog.Catalog;
import org.geoserver.config.GeoServerPersisterManager;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.config.util.XStreamPersisterFactory;
import org.geoserver.platform.GeoServerResourceLoader;
import org.geotools.util.logging.Logging;

/**
 * @author Francesco Izzi - CNR IMAA geoSDI Group
 * @email francesco.izzi@geosdi.org
 * 
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class GeoserverReceiverMQ implements MessageListener {

    static Logger LOGGER = Logging.getLogger("org.geoserver.config");
    public final String brokerURL;
    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageConsumer consumer;
    protected GeoserverSenderMQ geoserverSender;
    private ReceiverOperationFactory receiverOperationFactory;
    GeoServerResourceLoader rl;
    XStreamPersisterFactory xpf = new XStreamPersisterFactory();
    Catalog catalog;
    XStreamPersister xp;
    GeoServerPersisterManager gsp;

    public GeoserverReceiverMQ(GeoServerResourceLoader rl, Catalog catalog,
            String brokerURL, GeoserverSenderMQ geoserverSender,
            ReceiverOperationFactory receiverOperationFactory)
            throws JMSException {

        this.rl = rl;
        this.catalog = catalog;
        this.catalog.setResourceLoader(rl);
        this.brokerURL = brokerURL;
        this.geoserverSender = geoserverSender;
        this.receiverOperationFactory = receiverOperationFactory;

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

    @Override
    public void onMessage(Message message) {
        try {
            LOGGER.info(this.geoserverSender.toString());
            if (message.getStringProperty("sender").equals(this.geoserverSender.toString())) {
                LOGGER.info("Nothing to do");
            } else if (!this.geoserverSender.isWorking()) {
                String msgTypeContent = message.getStringProperty("type");
                IReceiverOperation receiverOperation =
                        this.receiverOperationFactory.getReceiverOperation(msgTypeContent);
                if (receiverOperation != null) {
                    receiverOperation.executeMessageContent(message);
                } else {
                    LOGGER.log(Level.WARNING, "Invalid message received or this "
                            + "geoserver isWorking...: {0}", msgTypeContent);
                }
            }
        } catch (JMSException e) {
            System.out.println("Caught:" + e);
            e.printStackTrace();
        }

    }
}
