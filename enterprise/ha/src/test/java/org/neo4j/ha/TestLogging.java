/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.ha;

import static junit.framework.Assert.assertTrue;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestLogging
{
    String masterDir, slaveDir;
    GraphDatabaseAPI master, slave;
    
    @Before
    public void before() throws Exception
    {
        LocalhostZooKeeperCluster zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        File dir = TargetDirectory.forTest( getClass() ).directory( "dbs", true );
        
        masterDir = new File( dir, "0" ).getAbsolutePath();
        master = (GraphDatabaseAPI) new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( masterDir )
                .setConfig( HaSettings.server, "localhost:6361" )
                .setConfig( HaSettings.server_id, "0" )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .newGraphDatabase();

        slaveDir = new File( dir, "1" ).getAbsolutePath();
        slave = (GraphDatabaseAPI) new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( slaveDir )
                .setConfig( HaSettings.server, "localhost:6362" )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .newGraphDatabase();
    }

    @After
    public void after() throws Exception
    {
        if ( slave != null )
            slave.shutdown();
        if ( master != null )
            master.shutdown();
    }
    
    @Test
    public void makeSureMessagesAreLoggedAfterInitialStoreCopy() throws Exception
    {
        slave.shutdown();
        slave = null;
        master.shutdown();
        master = null;
        
        assertMessagesLogContains( slaveDir, "STARTUP diagnostics" );
    }
    
    @Test
    public void makeSureMessagesAreLoggedAfterBrokerReconnect() throws Exception
    {
        ((HighlyAvailableGraphDatabase)slave).getBroker().restart();
        // Just do something that we know will write something to the log
        slave.getXaDataSourceManager().getNeoStoreDataSource().rotateLogicalLog();
        
        assertMessagesLogContains( slaveDir, "Rotating [" );
    }
    
    private void assertMessagesLogContains( String slaveDir, String expectingToFind )
    {
        boolean found = false;
        for ( String line : IteratorUtil.asIterable( new File( slaveDir, StringLogger.DEFAULT_NAME ) ) )
            if ( line.contains( expectingToFind ) )
                found = true;
        assertTrue( "Expected to find at least one log message including '" + expectingToFind + "' but didn't", found );
    }
}
