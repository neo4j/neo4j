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

package org.neo4j.server.enterprise;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.ha.LocalhostZooKeeperCluster.standardZoo;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.database.Database;
import org.neo4j.server.rrd.JobScheduler;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestHaDatabaseWithRrd
{
    private LocalhostZooKeeperCluster zoo;
    
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
    @Ignore
    public void startHaGraphDatabaseWithRrd() throws Exception
    {
//        String dir = TargetDirectory.forTest( getClass() ).directory( "rrd", true ).getAbsolutePath();
//        Map<String, String> config = stringMap(
//                HaSettings.server_id.name(), "1",
//                HaSettings.coordinators.name(), zoo.getConnectionString() );
//        
//        Configuration serverConfig = new MapConfiguration( stringMap(
//                Configurator.RRDB_LOCATION_PROPERTY_KEY, new File( dir, "rrd" ).getAbsolutePath() ) );
//        
//        Database db = new EnterpriseDatabase(  );
//        
//        RrdDb rrd = new RrdFactory( serverConfig ).createRrdDbAndSampler( db, noScheduling() );
//        db.setRrdDb( rrd );
//        
//        doTransaction( db );
//        
//        db.shutdown();
    }

    private void doTransaction( Database db )
    {
        Node node = null;
        Transaction tx = db.getGraph().beginTx();
        String key = "name";
        String value = "Test";
        try
        {
            node = db.getGraph().createNode();
            node.setProperty( key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        
        assertEquals( value, node.getProperty( key ) );
    }

    private JobScheduler noScheduling()
    {
        return new JobScheduler()
        {
            @Override
            public void scheduleAtFixedRate( Runnable job, String jobName, long delay, long period )
            {
            }
        };
    }
}
