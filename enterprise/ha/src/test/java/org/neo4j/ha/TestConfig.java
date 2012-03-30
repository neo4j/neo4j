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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.zookeeper.ZooClient;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperBroker;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperClusterClient;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

import static org.neo4j.test.TargetDirectory.*;
import static org.neo4j.test.ha.LocalhostZooKeeperCluster.*;

public class TestConfig
{
    private LocalhostZooKeeperCluster zoo;
    private final TargetDirectory dir = forTest( getClass() );

    @Before
    public void doBefore() throws Exception
    {
        zoo = standardZoo( getClass() );
    }

    @After
    public void doAfter() throws Exception
    {
        zoo.shutdown();
    }

    @Test
    public void testZkSessionTimeout() throws Exception
    {
        long timeout = 80000; // Default is 5000
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new EnterpriseGraphDatabaseFactory().
            newHighlyAvailableDatabaseBuilder( dir.directory( "zkTimeout" ).getAbsolutePath() ).
            setConfig( HaSettings.server_id, "1" ).
            setConfig( HaSettings.coordinators, zoo.getConnectionString() ).
            setConfig( HaSettings.zk_session_timeout, ""+timeout ).
            newGraphDatabase();
            
        ZooKeeperBroker broker = (ZooKeeperBroker) db.getBroker();

        // Test ZooClient

        Field f = broker.getClass().getDeclaredField( "zooClient" );
        f.setAccessible( true ); // private
        ZooClient zooClient = (ZooClient) f.get( broker );
        Method m = zooClient.getClass().getSuperclass().getDeclaredMethod(
                "getSessionTimeout" );
        m.setAccessible( true ); // protected
        Integer timeoutParsed = (Integer) m.invoke( zooClient );
        Assert.assertEquals( timeout, timeoutParsed.intValue() );

        // Test ClusterClient, reuse f, m and timeoutParsed

        f = db.getClass().getDeclaredField( "clusterClient" );
        f.setAccessible( true ); // private
        ZooKeeperClusterClient clusterClient = (ZooKeeperClusterClient) f.get( db );
        m = clusterClient.getClass().getSuperclass().getDeclaredMethod(
                "getSessionTimeout" );
        m.setAccessible( true ); // protected
        timeoutParsed = (Integer) m.invoke( clusterClient );
        Assert.assertEquals( timeout, timeoutParsed.intValue() );

        // Done, Shutdown

        db.shutdown();
    }
}
