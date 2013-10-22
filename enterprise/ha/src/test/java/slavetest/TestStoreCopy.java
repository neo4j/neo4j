/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package slavetest;

import java.io.File;
import java.io.FileFilter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.RepairKit;

import static org.apache.commons.io.FileUtils.copyFileToDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.ha.SlaveStoreWriter.COPY_FROM_MASTER_TEMP;
import static org.neo4j.kernel.impl.util.FileUtils.deleteFiles;
import static org.neo4j.kernel.impl.util.StringLogger.DEFAULT_NAME;
import static org.neo4j.test.ha.ClusterManager.clusterWithAdditionalClients;

/**
 * Check sandboxed copy of stores. The before and after methods
 * as they are form a complete test, so a test method with an empty
 * body would still perform a valid test.
 */
public class TestStoreCopy extends AbstractClusterTest
{
    /**
     * Checks that leftovers in the sandbox directory are discarded on store
     * copy.
     *
     * @throws Exception
     */
    @Test
    public void sandboxIsOverwritten() throws Throwable
    {
        RepairKit slaveDown = cluster.shutdown( slave );

        long secondNodeId = createIndexedNode( cluster.getMaster(), KEY2, VALUE2 );

        copyFileToDirectory( new File( slaveDir, "neostore" ), slaveTempCopyDir, false );
        copyFileToDirectory( new File( slaveDir, "neostore.propertystore.db" ), slaveTempCopyDir, false );
        assertEquals( "Found these files:" + filesAsString( slaveTempCopyDir ), 3,
                slaveTempCopyDir.listFiles( DISREGARD_ACTIVE_LOG_FILES ).length );

        deleteFiles( slaveDir, "neostore.*");

        slave = slaveDown.repair();

        assertNodeAndIndexingExists( slave, nodeId, KEY, VALUE );
        assertNodeAndIndexingExists( slave, secondNodeId, KEY2, VALUE2 );
    }
    
    @Before
    public void simpleSanityCheck() throws Exception
    {
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );

        slave = cluster.getAnySlave();
        slaveDir = cluster.getStoreDir( slave );
        slaveTempCopyDir = new File( slaveDir, COPY_FROM_MASTER_TEMP );

        Transaction transaction = slave.beginTx();
        try
        {
            assertEquals( VALUE, slave.getNodeById( nodeId ).getProperty( KEY ) );
        }
        finally
        {
            transaction.finish();
        }
    }
    
    @After
    public void assertSandboxLeftoverContents() throws Exception
    {
        /*
         * Make sure:
         *  - The sandbox directory exists
         *  - It only has one file in it (disregarding stray active log files)
         *  - It is the messages.log file
         */
        assertTrue( slaveTempCopyDir.exists() );
        assertTrue( slaveTempCopyDir.isDirectory() );
        assertEquals( 1, slaveTempCopyDir.listFiles( DISREGARD_ACTIVE_LOG_FILES ).length );
        assertEquals( DEFAULT_NAME, slaveTempCopyDir.listFiles( DISREGARD_ACTIVE_LOG_FILES )[0].getName() );
    }
    
    private static final String KEY = "foo", KEY2 = "foo2";
    private static final String VALUE = "bar", VALUE2 = "bar2";
    private static final FileFilter DISREGARD_ACTIVE_LOG_FILES = new FileFilter()
    {
        @Override
        public boolean accept( File file )
        {
            // Skip log files and tx files from temporary database
            return !("active_tx_log tm_tx_log.1 tm_tx_log.2").contains( file.getName() );
        }
    };
    
    private long nodeId;
    private HighlyAvailableGraphDatabase slave;
    private File slaveDir;
    private File slaveTempCopyDir;
    
    public TestStoreCopy()
    {
        super( clusterWithAdditionalClients( 2, 1 ) );
    }

    @Override
    protected void insertClusterMemberInitialData( GraphDatabaseService db, String name, int serverId )
    {
        // The first instance will create the indexed node and assign the nodeId variable.
        if ( serverId == 1 )
            nodeId = createIndexedNode( db, KEY, VALUE );
    }

    private long createIndexedNode( GraphDatabaseService db, String key, String value )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node n = db.createNode();
            n.setProperty( key, value );
            db.index().forNodes( key ).add( n, key, value );
            long nodeId = n.getId();
            tx.success();
            return nodeId;
        }
        finally
        {
            tx.finish();
        }
    }

    private String filesAsString( File directory )
    {
        StringBuilder builder = new StringBuilder();
        for ( File file : directory.listFiles( DISREGARD_ACTIVE_LOG_FILES ) )
        {
            builder.append( "\n" ).append( file.isDirectory() ? "/" : "" ).append( file.getName() );
        }
        return builder.toString();
    }

    private void assertNodeAndIndexingExists( HighlyAvailableGraphDatabase db, long nodeId, String key, Object value )
    {
        try (Transaction tx = db.beginTx())
        {
            Node node = db.getNodeById( nodeId );
            assertEquals( "Property '" + key + "'='" + value + "' mismatch on " + node + " for " + db,
                    value, node.getProperty( key ) );
            assertTrue( "Index '" + key + "' not found for " + db, db.index().existsForNodes( key ) );
            assertEquals( "Index '" + key + "'='" + value + "' mismatch on " + node + " for " + db,
                    node, db.index().forNodes( key ).get( key, value ).getSingle() );
        }
    }
}
