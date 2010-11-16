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

package org.neo4j.server.rrd;

import org.rrd4j.core.Sample;

import javax.management.MalformedObjectNameException;
import java.io.IOException;
import java.util.Date;

/**
 * Manages sampling the state of the database and storing the samples in a round
 * robin database instance.
 * <p/>
 * To add other data points, or change how the sampling is done, look at
 * <p/>
 * TODO: This currently handles both sampling and JMX-connection related stuff.
 * Should be broken into two classes.
 */
public class RrdSampler
{
//    // JMX bean names
//    // DEFINED ELSEWHERE -- don't inline here!
//    private static final String JMX_NEO4J_PRIMITIVE_COUNT = Primitives.NAME;
//    private static final String JMX_NEO4J_STORE_FILE_SIZES = StoreFile.NAME;
//    private static final String JMX_NEO4J_MEMORY_MAPPING = MemoryMapping.NAME;
//    private static final String JMX_NEO4J_TRANSACTIONS = TransactionManager.NAME;
//    private static final String JMX_NEO4J_KERNEL = Kernel.NAME;
//    private static final String JMX_NEO4J_LOCKING = LockManager.NAME;
//    private static final String JMX_NEO4J_CACHE = Cache.NAME;
//    private static final String JMX_NEO4J_CONFIGURATION = "Configuration";
//    private static final String JMX_NEO4J_XA_RESOURCES = XaManager.NAME;
//
//    // JMX Attribute names
//    private static final String JMX_ATTR_NODE_COUNT = "NumberOfNodeIdsInUse";
//    private static final String JMX_ATTR_RELATIONSHIP_COUNT = "NumberOfRelationshipIdsInUse";
//    private static final String JMX_ATTR_PROPERTY_COUNT = "NumberOfPropertyIdsInUse";
//    private static final String JMX_ATTR_HEAP_MEMORY = "HeapMemoryUsage";
//
//    public static final String NODE_CACHE_SIZE = "node_cache_size";
//    public static final String NODE_COUNT = "node_count";
//    public static final String RELATIONSHIP_COUNT = "relationship_count";
//    public static final String PROPERTY_COUNT = "property_count";


    /**
     * The current sampling object. This is created when calling #start().
     */
    private Sample sample;
    private Sampleable[] samplables;

    /**
     * Update task. This is is triggered on a regular interval to record new
     * data points.
     */
    // server.schedule("rrd", updateTask, 0, 3000)

//    private TimerTask updateTask = new TimerTask()
//    {
//        public void run()
//        {
//            if ( !running )
//            {
//                this.cancel();
//            } else
//            {
//                try
//                {
//                    updateSample();
//                } catch ( Exception e )
//                {
//                    throw new RuntimeException( "DELETE ME PLEASE" );
//
//                }
//            }
//        }
//    };
//
//    // MANAGEMENT BEANS
//
//    private ObjectName memoryName;
//    private ObjectName primitivesName;
//    private ObjectName storeSizesName;
//    private ObjectName transactionsName;
//    private ObjectName memoryMappingName;
//    private ObjectName kernelName;
//    private ObjectName lockingName;
//    private ObjectName cacheName;
//    private ObjectName xaResourcesName;

    /**
     * Keep track of whether to run the update task or not.
     */
//    private boolean running = false;
//    private AbstractGraphDatabase graphDb;
//    public MBeanServerConnection mbeanServer;
    protected RrdSampler( /*AbstractGraphDatabase graphDb,*/ Sample sample,
                          Sampleable... samplables ) throws MalformedObjectNameException
    {
//        this.graphDb = graphDb;
        this.sample = sample;
//        memoryName = new ObjectName( "java.lang:type=Memory" );
//        loadMBeanNames();
//        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        this.samplables = samplables;
    }

    /**
     * Start the data collecting, creating a central round-robin database if one
     * does not exist.
     */
    //  public void start()
//    {
//        try
//        {
//            if ( running == false )
//            {
//                running = true;
//                sample = RrdManager.getRrdDB().createSample();
//                Timer timer = new Timer( "rrd" );
//
//                timer.scheduleAtFixedRate( updateTask, 0, 3000 );
//            }
//        }
//        catch ( IOException e )
//        {
//            throw new RuntimeException(
//                    "IO Error trying to access round robin database path. See nested exception.",
//                    e );
//        }
    //   }
//
//    /**
//     * Stop the data collecting.
//     */
//    public void stop()
//    {
//        running = false;
//    }

    /*
     * This will update the ObjectName attributes in the class. This is done
     * like this because the name of the neo4j mbeans will change each time the
     * neo4j kernel is restarted.
     */

//    private void loadMBeanNames() throws MalformedObjectNameException
//    {
//        // Grab relevant jmx management beans
//        ObjectName neoQuery = graphDb.getManagementBean( Kernel.class ).getMBeanQuery();
//        String instance = neoQuery.getKeyProperty( "instance" );
//        String baseName = neoQuery.getDomain() + ":instance=" + instance + ",name=";
//
//        primitivesName = new ObjectName( baseName + JMX_NEO4J_PRIMITIVE_COUNT );
//        storeSizesName = new ObjectName( baseName + JMX_NEO4J_STORE_FILE_SIZES );
//        transactionsName = new ObjectName( baseName + JMX_NEO4J_TRANSACTIONS );
//        memoryMappingName = new ObjectName( baseName + JMX_NEO4J_MEMORY_MAPPING );
//        kernelName = new ObjectName( baseName + JMX_NEO4J_KERNEL );
//        lockingName = new ObjectName( baseName + JMX_NEO4J_LOCKING );
//        cacheName = new ObjectName( baseName + JMX_NEO4J_CACHE );
//        xaResourcesName = new ObjectName( baseName + JMX_NEO4J_XA_RESOURCES );
//    }

    /*
     * This method is called each time we want a snapshot of the current system
     * state. Data sources to work with are defined in
     * {@link RrdManager#getRrdDB()}
     */
    public Sample updateSample()
    {
        try
        {
            sample.setTime( new Date().getTime() );
            for ( Sampleable samplable : samplables )
            {
                sample.setValue( samplable.getName(), samplable.getValue() );
            }
//            sample.setValue( NODE_CACHE_SIZE, 0d );
//
//            sample.setValue( NODE_COUNT, (Long)mbeanServer.getAttribute( primitivesName, JMX_ATTR_NODE_COUNT ) );
//            sample.setValue( RELATIONSHIP_COUNT, (Long)mbeanServer.getAttribute( primitivesName, JMX_ATTR_RELATIONSHIP_COUNT ) );
//            sample.setValue( PROPERTY_COUNT, (Long)mbeanServer.getAttribute( primitivesName, JMX_ATTR_PROPERTY_COUNT ) );
//
//            sample.setValue(
//                    RrdManager.MEMORY_PERCENT,
//                    ( ( (Long)( (CompositeDataSupport)mbeanServer.getAttribute(
//                            memoryName, JMX_ATTR_HEAP_MEMORY ) ).get( "used" ) + 0.0d ) / (Long)( (CompositeDataSupport)mbeanServer.getAttribute(
//                            memoryName, JMX_ATTR_HEAP_MEMORY ) ).get( "max" ) ) * 100 );
//

            sample.update();
            return sample;
        }
        catch ( IOException e )
        {
            throw new RuntimeException(
                    "IO Error trying to access round robin database path. See nested exception.",
                    e );
        }

    }
}
