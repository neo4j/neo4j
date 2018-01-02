/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.kernel.impl.api.integrationtest.UniquenessConstraintValidationConcurrencyIT.createNode;
import static org.neo4j.test.OtherThreadRule.isWaiting;

public class UniquenessConstraintValidationHAIT
{
    private static final Label LABEL = label( "Label1" );
    private static final String PROPERTY_KEY = "key1";

    @Rule
    public final OtherThreadRule<Void> otherThread = new OtherThreadRule<>();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withInitialDataset( uniquenessConstraint( LABEL, PROPERTY_KEY ) );

    @Test
    public void shouldAllowCreationOfNonConflictingDataOnSeparateHosts() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        Future<Boolean> created;

        try ( Transaction tx = slave1.beginTx() )
        {
            slave1.createNode( LABEL ).setProperty( PROPERTY_KEY, "value1" );

            created = otherThread.execute( createNode( slave2, LABEL.name(), PROPERTY_KEY, "value2" ) );
            tx.success();
        }

        // then
        assertTrue( "creating non-conflicting data should pass", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingDataOnSeparateHosts() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        Future<Boolean> created;
        try ( Transaction tx = slave1.beginTx() )
        {
            slave1.createNode( LABEL ).setProperty( PROPERTY_KEY, "value3" );

            created = otherThread.execute( createNode( slave2, LABEL.name(), PROPERTY_KEY, "value3" ) );

            assertThat( otherThread, isWaiting() );

            tx.success();
        }


        // then
        assertFalse( "creating violating data should fail", created.get() );
    }

    @Test
    public void shouldPreventConcurrentCreationOfConflictingNonStringPropertyOnMasterAndSlave() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // when
        Future<Boolean> created;
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode( LABEL ).setProperty( PROPERTY_KEY, 0x0099CC );

            created = otherThread.execute( createNode( slave, LABEL.name(), PROPERTY_KEY, 0x0099CC ) );

            assertThat( otherThread, isWaiting() );

            tx.success();
        }

        // then
        assertFalse( "creating violating data should fail", created.get() );
    }

    @Test
    public void shouldAllowOtherHostToCompleteIfFirstHostRollsBackTransaction() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();

        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        HighlyAvailableGraphDatabase slave2 = cluster.getAnySlave( /*except:*/slave1 );

        // when
        Future<Boolean> created;

        try ( Transaction tx = slave1.beginTx() )
        {
            slave1.createNode( LABEL ).setProperty( PROPERTY_KEY, "value4" );

            created = otherThread.execute( createNode( slave2, LABEL.name(), PROPERTY_KEY, "value4" ) );

            assertThat( otherThread, isWaiting() );

            tx.failure();
        }


        // then
        assertTrue( "creating data that conflicts only with rolled back data should pass", created.get() );
    }

    private static Listener<GraphDatabaseService> uniquenessConstraint( final Label label, final String propertyKey )
    {
        return new Listener<GraphDatabaseService>()
        {
            @Override
            public void receive( GraphDatabaseService db )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();

                    tx.success();
                }
            }
        };
    }
}
