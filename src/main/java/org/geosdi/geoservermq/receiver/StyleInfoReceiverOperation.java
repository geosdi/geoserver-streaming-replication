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
import static org.geosdi.geoservermq.receiver.AbstractReceiverOperation.LOGGER;
import org.geosdi.geoservermq.utility.DependencyBag;
import org.geoserver.catalog.CascadeDeleteVisitor;
import org.geoserver.catalog.StyleInfo;
import org.geoserver.catalog.WorkspaceInfo;

/**
 * @author Francesco Izzi - CNR IMAA geoSDI Group
 * @email francesco.izzi@geosdi.org
 *
 * @author Nazzareno Sileno - CNR IMAA geoSDI Group
 * @email nazzareno.sileno@geosdi.org
 */
public class StyleInfoReceiverOperation extends AbstractReceiverOperation<StyleInfo> {

    public StyleInfoReceiverOperation(DependencyBag dependencyBag) {
        super(dependencyBag);
    }

    @Override
    void executeModifyOperation(StyleInfo catalogInfo) {
        LOGGER.log(Level.INFO, "Adding ... {0}", catalogInfo.getId());
        StyleInfo theOriginal = super.dependencyBag.getCatalog().getStyle(catalogInfo.getId());
        try {
            super.dependencyBag.getMapper().map(catalogInfo, theOriginal);
        } catch (Exception e) {
        }
        WorkspaceInfo workspaceInfo = catalogInfo.getWorkspace();
        if (workspaceInfo != null && workspaceInfo.getId() != null && !workspaceInfo.getId().trim().equals("")) {
            WorkspaceInfo theOriginalWorkspace = super.dependencyBag.getCatalog().getWorkspace(theOriginal.getWorkspace().getId());
            theOriginal.setWorkspace(theOriginalWorkspace);
        }
        LOGGER.log(Level.INFO, "Vecchio style name: " + theOriginal.getName());
        LOGGER.log(Level.INFO, "Nuovo style name: " + catalogInfo.getName());
        LOGGER.log(Level.INFO, "Modify Style in save");
        super.dependencyBag.getCatalog().save(theOriginal);
//        if (catalogInfo.getName().equals(theOriginal.getName())) {
//            super.dependencyBag.getMapper().map(catalogInfo, theOriginal);
//            LOGGER.log(Level.INFO, "Modify Style in save");
//            super.dependencyBag.getCatalog().save(theOriginal);
//        } else {
//            LOGGER.log(Level.INFO, "Modify Style in remove - add");
//            super.dependencyBag.getMapper().map(catalogInfo, theOriginal);
//            super.dependencyBag.getCatalog().remove(catalogInfo);
//            super.dependencyBag.getCatalog().save(theOriginal);
//        }
    }

    @Override
    void executeAddOperation(StyleInfo catalogInfo) {
        LOGGER.log(Level.INFO, "Adding ... {0}", catalogInfo.getId());
        super.dependencyBag.getCatalog().add(catalogInfo);
    }

    @Override
    void executeRemoveOperation(StyleInfo catalogInfo) {
        LOGGER.log(Level.INFO, "Removing ... {0}", catalogInfo.getName());
        CascadeDeleteVisitor visitor = new CascadeDeleteVisitor(super.dependencyBag.getCatalog());
        catalogInfo.accept(visitor);
        System.out.println("Removing...: " + catalogInfo);
    }
}
