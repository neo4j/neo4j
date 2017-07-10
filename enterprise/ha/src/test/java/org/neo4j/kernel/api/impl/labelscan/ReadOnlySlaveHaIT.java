/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.labelscan;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.helpers.collection.Iterators.count;

public class ReadOnlySlaveHaIT
{
    private static LifeSupport life;
    private static ClusterManager.ManagedCluster cluster;

    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    @BeforeClass
    public static void setup()
    {
        life = new LifeSupport();
        setupCluster( life );
    }

    private static void setupCluster( LifeSupport life )
    {
        int masterId = 1;
        Map<String,IntFunction<String>> instanceConfigMap = MapUtil.genericMap(
                new HashMap<>(),
                GraphDatabaseSettings.read_only.name(), (IntFunction<String>) value -> value == masterId ? "false" : "true",
                HaSettings.slave_only.name(), (IntFunction<String>) value -> value == masterId ? "false" : "true" );

        ClusterManager.Builder builder = new ClusterManager.Builder( testDirectory.directory() )
                .withFirstInstanceId( masterId )
                .withSharedConfig( MapUtil.stringMap( HaSettings.tx_push_factor.name(), "2" ) )
                .withInstanceConfig( instanceConfigMap );
        ClusterManager clusterManager = builder.build();

        life.add( clusterManager );
        life.start();

        cluster = clusterManager.getCluster();
    }

    @AfterClass
    public static void tearDown()
    {
        if ( life != null )
        {
            life.shutdown();
        }
    }

    @Test
    public void readOnlySlavesMustBeAbleToPullUpdates() throws Exception
    {
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Label label = Label.label( "label" );

        try ( Transaction tx = master.beginTx() )
        {
            master.createNode( label );
            tx.success();
        }

        Iterable<HighlyAvailableGraphDatabase> allMembers = cluster.getAllMembers();
        for ( HighlyAvailableGraphDatabase member : allMembers )
        {
            try ( Transaction tx = member.beginTx() )
            {
                long count = count( member.findNodes( label ) );
                tx.success();
                assertEquals( 1, count );
            }
        }
    }

    @Test
    public void readOnlySlaveMustNotAcceptExternalUpdates() throws Exception
    {
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        Exception exception = null;
        try ( Transaction tx = slave.beginTx() )
        {
            slave.createNode();
            tx.success();
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            exception = e;
        }

        assertNotNull( "Expected read only slave to fail when trying to write", exception );
    }
}
