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

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestLogging
{
    @Test
    public void makeSureMessageAreLoggedAfterInitialStoreCopy() throws Exception
    {
        LocalhostZooKeeperCluster zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
        File dir = TargetDirectory.forTest( getClass() ).directory( "dbs", true );
        
        String masterDir = new File( dir, "0" ).getAbsolutePath();
        GraphDatabaseService master = new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( masterDir )
                .setConfig( HaSettings.server, "localhost:6361" )
                .setConfig( HaSettings.server_id, "0" )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .newGraphDatabase();

        String slaveDir = new File( dir, "1" ).getAbsolutePath();
        GraphDatabaseService slave = new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( slaveDir )
                .setConfig( HaSettings.server, "localhost:6362" )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .newGraphDatabase();
        
        slave.shutdown();
        master.shutdown();
        
        assertMessagesLogContains( slaveDir, "STARTUP diagnostics" );
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
