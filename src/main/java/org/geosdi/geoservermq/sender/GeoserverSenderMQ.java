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
package org.geosdi.geoservermq.sender;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
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
import org.geosdi.geoservermq.utility.OperationType;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.CatalogException;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerGroupInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.StyleInfo;
import org.geoserver.catalog.WMSLayerInfo;
import org.geoserver.catalog.WMSStoreInfo;
import org.geoserver.catalog.WorkspaceInfo;
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
 * @author Francesco Izzi - CNR IMAA geoSDI Group
 * @email francesco.izzi@geosdi.org
 *
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class GeoserverSenderMQ implements CatalogListener, ConfigurationListener {

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
            e.printStackTrace();
        }
    }

    @Override
    public void reloaded() {
        System.out
                .println("#################reloaded######################");
    }

    @Override
    public void handleAddEvent(CatalogAddEvent event) throws CatalogException {
        if (!this.isWorking) {
            System.out
                    .println("#################handleAddEvent######################");
            Object source = event.getSource();
            LOGGER.info(source.toString());
            try {
                if (source instanceof WorkspaceInfo) {
                    addWorkspace((WorkspaceInfo) source);
                } else if (source instanceof NamespaceInfo) {
                    addNamespace((NamespaceInfo) source);
                } else if (source instanceof DataStoreInfo) {
                    addDataStore((DataStoreInfo) source);
                } else if (source instanceof WMSStoreInfo) {
                    addWMSStore((WMSStoreInfo) source);
                } else if (source instanceof FeatureTypeInfo) {
                    addFeatureType((FeatureTypeInfo) source);
                } else if (source instanceof CoverageStoreInfo) {
                    addCoverageStore((CoverageStoreInfo) source);
                } else if (source instanceof CoverageInfo) {
                    addCoverage((CoverageInfo) source);
                } else if (source instanceof WMSLayerInfo) {
                    addWMSLayer((WMSLayerInfo) source);
                } else if (source instanceof LayerInfo) {
                    addLayer((LayerInfo) source);
                } else if (source instanceof StyleInfo) {
                    addStyle((StyleInfo) source);
                } else if (source instanceof LayerGroupInfo) {
                    addLayerGroup((LayerGroupInfo) source);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void handleRemoveEvent(CatalogRemoveEvent event)
            throws CatalogException {
        if (!isWorking) {
            System.out.println("#################handleRemoveEvent######################");
            Object source = event.getSource();
            try {
                if (source instanceof WorkspaceInfo) {
                    removeWorkspace((WorkspaceInfo) source);
                } else if (source instanceof NamespaceInfo) {
                    removeNamespace((NamespaceInfo) source);
                } else if (source instanceof DataStoreInfo) {
                    removeDataStore((DataStoreInfo) source);
                } else if (source instanceof FeatureTypeInfo) {
                    removeFeatureType((FeatureTypeInfo) source);
                } else if (source instanceof CoverageStoreInfo) {
                    removeCoverageStore((CoverageStoreInfo) source);
                } else if (source instanceof CoverageInfo) {
                    removeCoverage((CoverageInfo) source);
                } else if (source instanceof WMSStoreInfo) {
                    removeWMSStore((WMSStoreInfo) source);
                } else if (source instanceof LayerInfo) {
                    removeLayer((LayerInfo) source);
                } else if (source instanceof StyleInfo) {
                    removeStyle((StyleInfo) source);
                } else if (source instanceof LayerGroupInfo) {
                    removeLayerGroup((LayerGroupInfo) source);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void handlePostModifyEvent(CatalogPostModifyEvent event)
            throws CatalogException {
        if (!isWorking) {
            System.out.println("#################handlePostModifyEvent######################");

            Object source = event.getSource();
            try {
                if (source instanceof WorkspaceInfo) {
                    modifyWorkspace((WorkspaceInfo) source);
                } else if (source instanceof DataStoreInfo) {
                    modifyDataStore((DataStoreInfo) source);
                } else if (source instanceof WMSStoreInfo) {
                    modifyWMSStore((WMSStoreInfo) source);
                } else if (source instanceof NamespaceInfo) {
                    modifyNamespace((NamespaceInfo) source);
                } else if (source instanceof FeatureTypeInfo) {
                    modifyFeatureType((FeatureTypeInfo) source);
                } else if (source instanceof CoverageStoreInfo) {
                    modifyCoverageStore((CoverageStoreInfo) source);
                } else if (source instanceof CoverageInfo) {
                    modifyCoverage((CoverageInfo) source);
                } else if (source instanceof WMSLayerInfo) {
                    modifyWMSLayer((WMSLayerInfo) source);
                } else if (source instanceof LayerInfo) {
                    modifyLayer((LayerInfo) source);
                } else if (source instanceof StyleInfo) {
                    modifyStyle((StyleInfo) source);
                } else if (source instanceof LayerGroupInfo) {
                    modifyLayerGroup((LayerGroupInfo) source);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void handleModifyEvent(CatalogModifyEvent event)
            throws CatalogException {
        System.out
                .println("#################handleModifyEvent######################");
    }

    @Override
    public void handleGlobalChange(GeoServerInfo global, List<String> propertyNames,
            List<Object> oldValues, List<Object> newValues) {
        LOGGER.info("#################handleGlobalChange######################");

    }

    @Override
    public void handlePostGlobalChange(GeoServerInfo global) {
        System.out
                .println("#################handlePostGlobalChange######################");

    }

    @Override
    public void handleLoggingChange(LoggingInfo logging,
            List<String> propertyNames, List<Object> oldValues,
            List<Object> newValues) {
        System.out
                .println("#################handleLoggingChange######################");

    }

    @Override
    public void handlePostLoggingChange(LoggingInfo logging) {
        System.out
                .println("#################handlePostLoggingChange######################");

    }

    @Override
    public void handleServiceChange(ServiceInfo service,
            List<String> propertyNames, List<Object> oldValues,
            List<Object> newValues) {
        System.out
                .println("#################handleServiceChange######################");

    }

    @Override
    public void handlePostServiceChange(ServiceInfo service) {
        System.out
                .println("#################handlePostServiceChange######################");

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

    private void sendMessage(String descOperation, Object objToTransfer, String type,
            OperationType operationType) throws IOException, JMSException {
        if (!isWorking) {
            LOGGER.log(Level.FINE, "CLUSTER Message - {0}", descOperation);
            File file = File.createTempFile("stream", ".dat");
            persist(objToTransfer, file);
            String messageStr = FileUtils.readFileToString(file);
            Message message = session.createTextMessage(messageStr);
            message.setStringProperty("type", type);
            message.setStringProperty("sender", super.toString());
            message.setStringProperty("operation", operationType.name());
            LOGGER.log(Level.FINE, "MESSAGE : {0}", messageStr);
            producer.send(message);
        }
    }

    // workspace
    void addWorkspace(WorkspaceInfo w) throws IOException, JMSException {
        this.sendMessage("Persisting Workspace " + w.getName(), w,
                "WorkspaceInfo", OperationType.ADD);
    }

    // layers
    void addNamespace(NamespaceInfo n) throws IOException, JMSException {
        this.sendMessage("Persisting namespace " + n.getName(), n,
                "NamespaceInfo", OperationType.ADD);
    }

    // layers
    void addLayer(LayerInfo l) throws IOException, JMSException {
        this.sendMessage("Persisting layer " + l.getName(), l,
                "LayerInfo", OperationType.ADD);
    }

    void addFeatureType(FeatureTypeInfo ft) throws IOException, JMSException {
        this.sendMessage("Persisting feature type " + ft.getName(), ft,
                "FeatureTypeInfo", OperationType.ADD);
    }

    void addCoverageStore(CoverageStoreInfo cs) throws IOException, JMSException {
        this.sendMessage("Persisting coverage store " + cs.getName(), cs,
                "CoverageStoreInfo", OperationType.ADD);
    }

    void addCoverage(CoverageInfo ci) throws IOException, JMSException {
        this.sendMessage("Persisting coverage " + ci.getName(), ci,
                "CoverageInfo", OperationType.ADD);
    }

    //datastores
    void addDataStore(DataStoreInfo ds) throws IOException, JMSException {
        this.sendMessage("Persisting datastore " + ds.getName(), ds,
                "DataStoreInfo", OperationType.ADD);
    }

    //datastores
    void addWMSStore(WMSStoreInfo wms) throws IOException, JMSException {
        this.sendMessage("Persisting wms store " + wms.getName(), wms,
                "WMSStoreInfo", OperationType.ADD);
    }

    //wms layers
    void addWMSLayer(WMSLayerInfo wms) throws IOException, JMSException {
        this.sendMessage("Persisting wms layer " + wms.getName(), wms,
                "WMSLayerInfo", OperationType.ADD);
    }

    //styles
    void addStyle(StyleInfo s) throws IOException, JMSException {
        this.sendMessage("Persisting style " + s.getName(), s,
                "StyleInfo", OperationType.ADD);
    }

    //layer groups
    void addLayerGroup(LayerGroupInfo lg) throws IOException, JMSException {
        this.sendMessage("Persisting layer group " + lg.getName(), lg,
                "LayerGroupInfo", OperationType.ADD);
    }

    //helpers
    void persist(Object o, File dir, String filename) throws IOException {
        persist(o, new File(dir, filename));
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

    public boolean isWorking() {
        return isWorking;
    }

    public void setIsWorking(boolean isWorking) {
        this.isWorking = isWorking;
    }

    private void modifyWorkspace(WorkspaceInfo ws) throws IOException, JMSException {
        this.sendMessage("Modifying workspace " + ws.getName(), ws,
                "WorkspaceInfo", OperationType.MODIFY);
    }

    void modifyDataStore(DataStoreInfo ds) throws IOException, JMSException {
        this.sendMessage("Modifying datastore " + ds.getName(), ds,
                "DataStoreInfo", OperationType.MODIFY);
    }

    void modifyWMSStore(WMSStoreInfo ds) throws IOException, JMSException {
        this.sendMessage("Modifying wms store " + ds.getName(), ds,
                "WMSStoreInfo", OperationType.MODIFY);
    }

    void modifyNamespace(NamespaceInfo ns) throws IOException, JMSException {
        this.sendMessage("Modifying namespace " + ns.getName(), ns,
                "NamespaceInfo", OperationType.MODIFY);
    }

    void modifyFeatureType(FeatureTypeInfo ft) throws IOException, JMSException {
        this.sendMessage("Modifying feature type " + ft.getName(), ft,
                "FeatureTypeInfo", OperationType.MODIFY);
    }

    void modifyCoverageStore(CoverageStoreInfo cs) throws IOException, JMSException {
        this.sendMessage("Modifying coverage store " + cs.getName(), cs,
                "CoverageStoreInfo", OperationType.MODIFY);
    }

    void modifyCoverage(CoverageInfo c) throws IOException, JMSException {
        this.sendMessage("Modifying coverage " + c.getName(), c,
                "CoverageInfo", OperationType.MODIFY);
    }

    void modifyWMSLayer(WMSLayerInfo wms) throws IOException, JMSException {
        this.sendMessage("Modifying wms layer " + wms.getName(), wms,
                "WMSLayerInfo", OperationType.MODIFY);
    }

    void modifyLayer(LayerInfo l) throws IOException, JMSException {
        this.sendMessage("Modifying layer " + l.getName(), l,
                "LayerInfo", OperationType.MODIFY);
    }

    void modifyStyle(StyleInfo s) throws IOException, JMSException {
        this.sendMessage("Modifying style " + s.getName(), s,
                "StyleInfo", OperationType.MODIFY);
    }

    void modifyLayerGroup(LayerGroupInfo lg) throws IOException, JMSException {
        this.sendMessage("Modifying layer group " + lg.getName(), lg,
                "LayerGroupInfo", OperationType.MODIFY);
    }

    private void removeWorkspace(WorkspaceInfo workspaceInfo) throws IOException, JMSException {
        this.sendMessage("Removing Workspace " + workspaceInfo.getName(), workspaceInfo,
                "WorkspaceInfo", OperationType.REMOVE);
    }

    private void removeNamespace(NamespaceInfo namespaceInfo) throws IOException, JMSException {
        this.sendMessage("Removing namespace " + namespaceInfo.getName(), namespaceInfo,
                "NamespaceInfo", OperationType.REMOVE);
    }

    private void removeLayer(LayerInfo layerInfo) throws IOException, JMSException {
        this.sendMessage("Removing layer " + layerInfo.getName(), layerInfo,
                "LayerInfo", OperationType.REMOVE);
    }

    private void removeFeatureType(FeatureTypeInfo featureTypeInfo) throws IOException, JMSException {
        this.sendMessage("Removing feature type " + featureTypeInfo.getName(), featureTypeInfo,
                "FeatureTypeInfo", OperationType.REMOVE);
    }

    private void removeDataStore(DataStoreInfo dataStoreInfo) throws IOException, JMSException {
        this.sendMessage("Removing datastore " + dataStoreInfo.getName(), dataStoreInfo,
                "DataStoreInfo", OperationType.REMOVE);
    }

    private void removeCoverageStore(CoverageStoreInfo coverageStoreInfo) throws IOException, JMSException {
        this.sendMessage("Removing coverage store " + coverageStoreInfo.getName(), coverageStoreInfo,
                "CoverageStoreInfo", OperationType.REMOVE);
    }

    private void removeCoverage(CoverageInfo coverageInfo) throws IOException, JMSException {
        this.sendMessage("Removing coverage " + coverageInfo.getName(), coverageInfo,
                "CoverageInfo", OperationType.REMOVE);
    }

    private void removeWMSStore(WMSStoreInfo ds) throws IOException, JMSException {
        this.sendMessage("Removing datastore " + ds.getName(), ds,
                "WMSStoreInfo", OperationType.REMOVE);
    }

    private void removeStyle(StyleInfo s) throws IOException, JMSException {
        this.sendMessage("Removing style " + s.getName(), s,
                "StyleInfo", OperationType.REMOVE);
    }

    private void removeLayerGroup(LayerGroupInfo lg) throws IOException, JMSException {
        this.sendMessage("Removing style " + lg.getName(), lg,
                "LayerGroupInfo", OperationType.REMOVE);
    }
    // Utility Methods
//    File file(WorkspaceInfo ws) throws IOException {
//        return new File(dir(ws), "workspace.xml");
//    }
//
//    File dir(WorkspaceInfo ws) throws IOException {
//        return dir(ws, false);
//    }
//
//    File dir(WorkspaceInfo ws, boolean create) throws IOException {
//        File d = rl.find("workspaces", ws.getName());
//        if (d == null && create) {
//            d = rl.createDirectory("workspaces", ws.getName());
//        }
//        return d;
//    }
}
