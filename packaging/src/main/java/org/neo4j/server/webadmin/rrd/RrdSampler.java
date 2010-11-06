/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.webadmin.rrd;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.mortbay.log.Log;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.management.Kernel;
import org.neo4j.management.Neo4jManager;
import org.neo4j.server.NeoServer;
import org.neo4j.server.webadmin.MBeanServerFactory;
import org.rrd4j.core.Sample;

/**
 * Manages sampling the state of the database and storing the samples in a round
 * robin database instance.
 * 
 * To add other data points, or change how the sampling is done, look at
 * {@link #updateSample(Sample)}.
 * 
 * TODO: This currently handles both sampling and JMX-connection related stuff.
 * Should be broken into two classes.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class RrdSampler {

    //
    // SINGLETON IMPLEMENTATION
    //

    public static final RrdSampler INSTANCE = new RrdSampler();

    // JMX bean names
    private static final String JMX_NEO4J_PRIMITIVE_COUNT = "Primitive count";
    private static final String JMX_NEO4J_STORE_FILE_SIZES = "Store file sizes";
    private static final String JMX_NEO4J_MEMORY_MAPPING = "Memory Mapping";
    private static final String JMX_NEO4J_TRANSACTIONS = "Transactions";
    private static final String JMX_NEO4J_KERNEL = "Kernel";
    private static final String JMX_NEO4J_LOCKING = "Locking";
    private static final String JMX_NEO4J_CACHE = "Cache";
    private static final String JMX_NEO4J_CONFIGURATION = "Configuration";
    private static final String JMX_NEO4J_XA_RESOURCES = "XA Resources";

    // JMX Attribute names
    private static final String JMX_ATTR_HEAP_MEMORY = "HeapMemoryUsage";

    /**
     * The current sampling object. This is created when calling #start().
     */
    private Sample sample;

    /**
     * Update task. This is is triggered on a regular interval to record new
     * data points.
     */
    private TimerTask updateTask = new TimerTask() {
        public void run() {
            if (!running) {
                this.cancel();
            } else {
                updateSample(sample);
            }
        }
    };

    /**
     * Keep track of whether to run the update task or not.
     */
    private boolean running = false;

    // MANAGEMENT BEANS
    private ObjectName memoryName;
    private ObjectName primitivesName = null;
    private Kernel kernel;
    private Neo4jManager manager;
    @SuppressWarnings("unused")
    private ObjectName storeFileSizesName;
    @SuppressWarnings("unused")
    private ObjectName transactionsName;
    @SuppressWarnings("unused")
    private ObjectName memoryMappingName;
    @SuppressWarnings("unused")
    private ObjectName kernelName;
    @SuppressWarnings("unused")
    private ObjectName lockingName;
    @SuppressWarnings("unused")
    private ObjectName cacheName;
    @SuppressWarnings("unused")
    private ObjectName configurationName;
    @SuppressWarnings("unused")
    private ObjectName xaResourcesName;

    //
    // CONSTRUCTOR
    //

    protected RrdSampler() {
        try {
            memoryName = new ObjectName("java.lang:type=Memory");
        } catch (MalformedObjectNameException e) {
            e.printStackTrace();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    //
    // PUBLIC
    //

    /**
     * Start the data collecting, creating a central round-robin database if one
     * does not exist.
     */
    public void start() {
        try {
            if (running == false) {
                running = true;
                sample = RrdManager.getRrdDB().createSample();
                Timer timer = new Timer("rrd");

                timer.scheduleAtFixedRate(updateTask, 0, 3000);
            }
        } catch (IOException e) {
            throw new RuntimeException("IO Error trying to access round robin database path. See nested exception.", e);
        }
    }

    /**
     * Stop the data collecting.
     */
    public void stop() {
        running = false;
    }

    //
    // INTERNALS
    //

    /**
     * This will update the ObjectName attributes in the class. This is done
     * like this because the name of the neo4j mbeans will change each time the
     * neo4j kernel is restarted.
     */
    protected void reloadMBeanNames() {
        try {
            GraphDatabaseService genericDb = NeoServer.server().database().db;

            if (genericDb instanceof EmbeddedGraphDatabase) {
                EmbeddedGraphDatabase db = (EmbeddedGraphDatabase) genericDb;

                // Grab relevant jmx management beans
                ObjectName neoQuery = db.getManagementBean(Kernel.class).getMBeanQuery();
                String instance = neoQuery.getKeyProperty("instance");
                String baseName = neoQuery.getDomain() + ":instance=" + instance + ",name=";

                primitivesName = new ObjectName(baseName + JMX_NEO4J_PRIMITIVE_COUNT);
                storeFileSizesName = new ObjectName(baseName + JMX_NEO4J_STORE_FILE_SIZES);
                transactionsName = new ObjectName(baseName + JMX_NEO4J_TRANSACTIONS);
                memoryMappingName = new ObjectName(baseName + JMX_NEO4J_MEMORY_MAPPING);
                kernelName = new ObjectName(baseName + JMX_NEO4J_KERNEL);
                lockingName = new ObjectName(baseName + JMX_NEO4J_LOCKING);
                cacheName = new ObjectName(baseName + JMX_NEO4J_CACHE);
                configurationName = new ObjectName(baseName + JMX_NEO4J_CONFIGURATION);
                xaResourcesName = new ObjectName(baseName + JMX_NEO4J_XA_RESOURCES);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method is called each time we want a snapshot of the current system
     * state. Data sources to work with are defined in
     * {@link RrdManager#getRrdDB()}
     */
    private void updateSample(Sample sample) {

        try {
            MBeanServerConnection server = MBeanServerFactory.getServer();
            kernel = ((EmbeddedGraphDatabase) NeoServer.server().database().db).getManagementBean(Kernel.class);
            manager = new Neo4jManager(kernel);
            reloadMBeanNames();

            sample.setTime(new Date().getTime());

            sample.setValue(RrdManager.NODE_CACHE_SIZE, 0d);

            if (primitivesName != null) {
                Long attribute = (Long) manager.getPrimitivesBean().getNumberOfNodeIdsInUse();
                sample.setValue(RrdManager.NODE_COUNT, attribute);

                sample.setValue(RrdManager.RELATIONSHIP_COUNT, (Long) manager.getPrimitivesBean().getNumberOfRelationshipIdsInUse());
                Log.debug("sampling node count: " + manager.getPrimitivesBean().getNumberOfNodeIdsInUse());

                sample.setValue(RrdManager.PROPERTY_COUNT, (Long) manager.getPrimitivesBean().getNumberOfPropertyIdsInUse());
            }

            if (memoryName != null) {
                sample
                        .setValue(RrdManager.MEMORY_PERCENT, (((Long) ((CompositeDataSupport) server.getAttribute(memoryName, JMX_ATTR_HEAP_MEMORY))
                                .get("used") + 0.0d) / (Long) ((CompositeDataSupport) server.getAttribute(memoryName, JMX_ATTR_HEAP_MEMORY)).get("max")) * 100);

            }

            sample.update();
        } catch (IOException e) {
            throw new RuntimeException("IO Error trying to access round robin database path. See nested exception.", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
