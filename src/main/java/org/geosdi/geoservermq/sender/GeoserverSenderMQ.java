/**
 *
 */
package org.geosdi.geoservermq.sender;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.io.FileUtils;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CatalogException;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.event.CatalogAddEvent;
import org.geoserver.catalog.event.CatalogListener;
import org.geoserver.catalog.event.CatalogModifyEvent;
import org.geoserver.catalog.event.CatalogPostModifyEvent;
import org.geoserver.catalog.event.CatalogRemoveEvent;
import org.geoserver.config.ConfigurationListener;
import org.geoserver.config.GeoServerInfo;
import org.geoserver.config.LoggingInfo;
import org.geoserver.config.ServiceInfo;
import org.geoserver.config.SettingsInfo;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.config.util.XStreamPersisterFactory;
import org.geoserver.platform.GeoServerResourceLoader;
import org.geotools.util.logging.Logging;

/**
 * @author fizzi
 *
 */
public class GeoserverSenderMQ implements CatalogListener,
        ConfigurationListener {

    static Logger LOGGER = Logging.getLogger("org.geoserver.config");
    public final String brokerURL;
    private ConnectionFactory factory;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    GeoServerResourceLoader rl;
    XStreamPersisterFactory xpf = new XStreamPersisterFactory();
    Catalog catalog;
    XStreamPersister xp;
    private boolean isWorking = false;

    public GeoserverSenderMQ(GeoServerResourceLoader rl, Catalog catalog, String brokerURL) {
        this.brokerURL = brokerURL;
        this.rl = rl;
        this.catalog = catalog;
        this.catalog.setResourceLoader(rl);
        xp = xpf.createXMLPersister();
        xp.setCatalog(catalog);
        xp.setEncryptPasswordFields(false);
        catalog.addListener(this);
        LOGGER.info("################### GeoserverActiveMQ ######################");
        initMQ();
    }

    private void initMQ() {
        factory = new ActiveMQConnectionFactory(brokerURL);
        try {
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic("GeoServer-Queue");
            producer = session.createProducer(destination);
        } catch (JMSException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void handleGlobalChange(GeoServerInfo global,
            List<String> propertyNames, List<Object> oldValues,
            List<Object> newValues) {
        LOGGER.info("#################handleGlobalChange######################");

    }

    public void handlePostGlobalChange(GeoServerInfo global) {
        System.out
                .println("#################handlePostGlobalChange######################");

    }

    public void handleLoggingChange(LoggingInfo logging,
            List<String> propertyNames, List<Object> oldValues,
            List<Object> newValues) {
        System.out
                .println("#################handleLoggingChange######################");

    }

    public void handlePostLoggingChange(LoggingInfo logging) {
        System.out
                .println("#################handlePostLoggingChange######################");

    }

    public void handleServiceChange(ServiceInfo service,
            List<String> propertyNames, List<Object> oldValues,
            List<Object> newValues) {
        System.out
                .println("#################handleServiceChange######################");

    }

    public void handlePostServiceChange(ServiceInfo service) {
        System.out
                .println("#################handlePostServiceChange######################");

    }

    public void handleAddEvent(CatalogAddEvent event) throws CatalogException {
        if (!this.isWorking) {
            System.out
                    .println("#################handleAddEvent######################");
            Object source = event.getSource();
            try {
                if (source instanceof LayerInfo) {
                    addLayer((LayerInfo) source);
                } else if (source instanceof FeatureTypeInfo) {
                    addFeatureType((FeatureTypeInfo) source);
                } else if (source instanceof DataStoreInfo) {
                    addDataStore((DataStoreInfo) source);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    // layers
    void addLayer(LayerInfo l) throws IOException, JMSException {
        if (!isWorking) {
            LOGGER.fine("CLUSTER Message  - Persisting layer " + l.getName());
            File file = File.createTempFile("test", ".dat");
            persist(l, file);
            String messageStr = FileUtils.readFileToString(file);
            Message message = session.createTextMessage(messageStr);
            message.setStringProperty("type", "LayerInfo");
            message.setStringProperty("sender", super.toString());
            message.setStringProperty("operation", "ADD");
            LOGGER.info("MESSAGE : " +messageStr);
            producer.send(message);
        }

    }

    void addFeatureType(FeatureTypeInfo ft) throws IOException, JMSException {
        if (!isWorking) {
            File file = File.createTempFile("test", ".dat");
            persist(ft, file);
            String messageStr = FileUtils.readFileToString(file);
            Message message = session.createTextMessage(messageStr);
            message.setStringProperty("type", "FeatureTypeInfo");
            message.setStringProperty("sender", super.toString());
            message.setStringProperty("operation", "ADD");
            LOGGER.info("MESSAGE : " +messageStr);
            producer.send(message);
        }
    }

    //datastores
    void addDataStore(DataStoreInfo ds) throws IOException, JMSException {
        if (!isWorking) {
            LOGGER.info("Persisting datastore " + ds.getName());
            File file = File.createTempFile("test", ".dat");
            persist(ds, file);
            String messageStr = FileUtils.readFileToString(file);
            Message message = session.createTextMessage(messageStr);
            message.setStringProperty("type", "DataStoreInfo");
            message.setStringProperty("sender", super.toString());
            message.setStringProperty("operation", "ADD");
            LOGGER.info("MESSAGE : " +messageStr);
            producer.send(message);
        }
    }

    public void handleRemoveEvent(CatalogRemoveEvent event)
            throws CatalogException {

        if (!isWorking) {
            System.out
                    .println("#################handleRemoveEvent######################");

            Object source = event.getSource();
            try {
                if (source instanceof LayerInfo) {
                    removeLayer((LayerInfo) source);
                } else if (source instanceof FeatureTypeInfo) {
                    removeFeatureType((FeatureTypeInfo) source);
                } else if (source instanceof DataStoreInfo) {
                    removeDataStore((DataStoreInfo) source, event);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }




    }

    public void handleModifyEvent(CatalogModifyEvent event)
            throws CatalogException {
        System.out
                .println("#################handleModifyEvent######################");

    }

    public void handlePostModifyEvent(CatalogPostModifyEvent event)
            throws CatalogException {
        System.out
                .println("#################handlePostModifyEvent######################");



    }

    public void reloaded() {
        System.out
                .println("#################reloaded######################");

    }

    void persist(Object o, File f) throws IOException {
        try {
            synchronized (xp) {
                //first save to a temp file
                File temp = new File(f.getParentFile(), f.getName() + ".tmp");
                if (temp.exists()) {
                    temp.delete();
                }

                BufferedOutputStream out = null;
                try {
                    out = new BufferedOutputStream(new FileOutputStream(temp));
                    xp.save(o, out);
                    out.flush();
                } finally {
                    if (out != null) {
                        org.apache.commons.io.IOUtils.closeQuietly(out);
                    }
                }
                rename(temp, f);

            }
            LOGGER.fine("Persisted " + o.getClass().getName() + " to " + f.getAbsolutePath());
        } catch (Exception e) {
            //catch any exceptions and send them back as CatalogExeptions
            String msg = "Error persisting " + o + " to " + f.getCanonicalPath();
            throw new CatalogException(msg, e);
        }
    }

    void rename(File source, File dest) throws IOException {
        // same path? Do nothing
        if (source.getCanonicalPath().equalsIgnoreCase(dest.getCanonicalPath())) {
            return;
        }

        // different path
        boolean win = System.getProperty("os.name").startsWith("Windows");
        if (win && dest.exists()) {
            //windows does not do atomic renames, and can not rename a file if the dest file
            // exists
            if (!dest.delete()) {
                throw new IOException("Could not delete: " + dest.getCanonicalPath());
            }
            source.renameTo(dest);
        } else {
            source.renameTo(dest);
        }
    }

    @Override
    public void handleSettingsAdded(SettingsInfo settings) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void handleSettingsModified(SettingsInfo settings, List<String> propertyNames, List<Object> oldValues, List<Object> newValues) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void handleSettingsPostModified(SettingsInfo settings) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void handleSettingsRemoved(SettingsInfo settings) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void handleServiceRemove(ServiceInfo service) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public boolean isWorking() {
        return isWorking;
    }

    public void setIsWorking(boolean isWorking) {
        this.isWorking = isWorking;
    }

    private void removeLayer(LayerInfo layerInfo) throws IOException, JMSException {

        LOGGER.fine("CLUSTER Message  - Persisting layer " + layerInfo.getName());
        File file = File.createTempFile("test", ".dat");
        persist(layerInfo, file);
        String messageStr = FileUtils.readFileToString(file);
        Message message = session.createTextMessage(messageStr);
        message.setStringProperty("type", "LayerInfo");
        message.setStringProperty("sender", super.toString());
        message.setStringProperty("operation", "REMOVE");
        LOGGER.info("MESSAGE : " +messageStr);
        producer.send(message);

    }

    private void removeFeatureType(FeatureTypeInfo featureTypeInfo) throws IOException, JMSException {
        LOGGER.fine("CLUSTER Message  - Remove feature type " + featureTypeInfo.getName());
        File file = File.createTempFile("test", ".dat");
        persist(featureTypeInfo, file);
        String messageStr = FileUtils.readFileToString(file);
        Message message = session.createTextMessage(messageStr);
        message.setStringProperty("type", "FeatureTypeInfo");
        message.setStringProperty("sender", super.toString());
        message.setStringProperty("operation", "REMOVE");
        LOGGER.info("MESSAGE : " +messageStr);
        producer.send(message);
    }

    private void removeDataStore(DataStoreInfo dataStoreInfo, CatalogRemoveEvent event) throws IOException, JMSException {
        LOGGER.fine("CLUSTER Message  - Remove datastore " + dataStoreInfo.getName());
        File file = File.createTempFile("test", ".dat");
        persist(dataStoreInfo, file);
        String messageStr = FileUtils.readFileToString(file);
        Message message = session.createTextMessage(messageStr);
        message.setStringProperty("type", "DataStoreInfo");
        message.setStringProperty("sender", super.toString());
        message.setStringProperty("operation", "REMOVE");
        LOGGER.info("MESSAGE : " +messageStr);
        producer.send(message);
    }
}
