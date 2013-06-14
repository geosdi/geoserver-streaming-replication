/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geoserver.config;

import java.io.IOException;
import org.geoserver.catalog.CoverageInfo;
import org.geoserver.catalog.CoverageStoreInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.FeatureTypeInfo;
import org.geoserver.catalog.LayerGroupInfo;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.NamespaceInfo;
import org.geoserver.catalog.ResourceInfo;
import org.geoserver.catalog.StoreInfo;
import org.geoserver.catalog.StyleInfo;
import org.geoserver.catalog.WMSLayerInfo;
import org.geoserver.catalog.WMSStoreInfo;
import org.geoserver.catalog.WorkspaceInfo;
import org.geoserver.config.util.XStreamPersister;
import org.geoserver.platform.GeoServerResourceLoader;

/**
 *
 * @author fizzi
 */
public class GeoServerPersisterManager extends GeoServerPersister {

    public GeoServerPersisterManager(GeoServerResourceLoader rl, XStreamPersister xp) {
        super(rl, xp);
    }

    @Override
    public void removeDataStore(DataStoreInfo ds) throws IOException {
        super.removeDataStore(ds); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addWorkspace(WorkspaceInfo ws) throws IOException {
        super.addWorkspace(ws); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void renameWorkspace(WorkspaceInfo ws, String newName) throws IOException {
        super.renameWorkspace(ws, newName); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyWorkspace(WorkspaceInfo ws) throws IOException {
        super.modifyWorkspace(ws); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeWorkspace(WorkspaceInfo ws) throws IOException {
        super.removeWorkspace(ws); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addNamespace(NamespaceInfo ns) throws IOException {
        super.addNamespace(ns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyNamespace(NamespaceInfo ns) throws IOException {
        super.modifyNamespace(ns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeNamespace(NamespaceInfo ns) throws IOException {
        super.removeNamespace(ns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addDataStore(DataStoreInfo ds) throws IOException {
        super.addDataStore(ds); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void renameStore(StoreInfo s, String newName) throws IOException {
        super.renameStore(s, newName); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyDataStore(DataStoreInfo ds) throws IOException {
        super.modifyDataStore(ds); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addFeatureType(FeatureTypeInfo ft) throws IOException {
        super.addFeatureType(ft); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void renameResource(ResourceInfo r, String newName) throws IOException {
        super.renameResource(r, newName); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyFeatureType(FeatureTypeInfo ft) throws IOException {
        super.modifyFeatureType(ft); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeFeatureType(FeatureTypeInfo ft) throws IOException {
        super.removeFeatureType(ft); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addCoverageStore(CoverageStoreInfo cs) throws IOException {
        super.addCoverageStore(cs); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyCoverageStore(CoverageStoreInfo cs) throws IOException {
        super.modifyCoverageStore(cs); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeCoverageStore(CoverageStoreInfo cs) throws IOException {
        super.removeCoverageStore(cs); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addCoverage(CoverageInfo c) throws IOException {
        super.addCoverage(c); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyCoverage(CoverageInfo c) throws IOException {
        super.modifyCoverage(c); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeCoverage(CoverageInfo c) throws IOException {
        super.removeCoverage(c); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addWMSStore(WMSStoreInfo wms) throws IOException {
        super.addWMSStore(wms); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyWMSStore(WMSStoreInfo ds) throws IOException {
        super.modifyWMSStore(ds); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeWMSStore(WMSStoreInfo ds) throws IOException {
        super.removeWMSStore(ds); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addWMSLayer(WMSLayerInfo wms) throws IOException {
        super.addWMSLayer(wms); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyWMSLayer(WMSLayerInfo wms) throws IOException {
        super.modifyWMSLayer(wms); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeWMSLayer(WMSLayerInfo c) throws IOException {
        super.removeWMSLayer(c); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addLayer(LayerInfo l) throws IOException {
        super.addLayer(l); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyLayer(LayerInfo l) throws IOException {
        super.modifyLayer(l); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeLayer(LayerInfo l) throws IOException {
        super.removeLayer(l); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addStyle(StyleInfo s) throws IOException {
        super.addStyle(s); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void renameStyle(StyleInfo s, String newName) throws IOException {
        super.renameStyle(s, newName); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyStyle(StyleInfo s) throws IOException {
        super.modifyStyle(s); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeStyle(StyleInfo s) throws IOException {
        super.removeStyle(s); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addLayerGroup(LayerGroupInfo lg) throws IOException {
        super.addLayerGroup(lg); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void renameLayerGroup(LayerGroupInfo lg, String newName) throws IOException {
        super.renameLayerGroup(lg, newName); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void modifyLayerGroup(LayerGroupInfo lg) throws IOException {
        super.modifyLayerGroup(lg); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeLayerGroup(LayerGroupInfo lg) throws IOException {
        super.removeLayerGroup(lg); //To change body of generated methods, choose Tools | Templates.
    }
}
