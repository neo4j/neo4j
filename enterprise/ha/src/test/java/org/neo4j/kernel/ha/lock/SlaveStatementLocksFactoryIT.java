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
package org.neo4j.kernel.ha.lock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.lock.trace.LockRecord;
import org.neo4j.kernel.ha.lock.trace.RecordingLockTracer;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SlaveStatementLocksFactoryIT
{
    private static final Label testLabel = Label.label( "testLabel" );
    private static final String testProperty = "testProperty";

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( GraphDatabaseFacadeFactory.Configuration.tracer, "slaveLocksTracer" )
            .withSharedSetting( HaSettings.tx_push_factor, "2" );
    private ClusterManager.ManagedCluster managedCluster;

    @Before
    public void setUp() throws Exception
    {
        managedCluster = clusterRule.startCluster();
    }

    @Test
    public void acquireSharedLocksDuringSlaveWriteTx()
    {
        HighlyAvailableGraphDatabase anySlave = managedCluster.getAnySlave();
        HighlyAvailableGraphDatabase master = managedCluster.getMaster();

        createSingleTestLabeledNode( master );

        LockRecord sharedLabelLock = LockRecord.of( false, ResourceTypes.LABEL, 0 );
        List<LockRecord> requestedLocks = getRequestedLocks( anySlave );
        assertFalse( requestedLocks.contains( sharedLabelLock ) );

        createSingleTestLabeledNode( anySlave );

        assertTrue( requestedLocks.contains( sharedLabelLock ) );
    }

    @Test
    public void doNotAcquireSharedLocksDuringSlaveReadTx()
    {
        HighlyAvailableGraphDatabase anySlave = managedCluster.getAnySlave();
        HighlyAvailableGraphDatabase master = managedCluster.getMaster();

        try ( Transaction tx = master.beginTx() )
        {
            Node node = master.createNode( testLabel );
            node.setProperty( testProperty, "a" );
            tx.success();
        }

        createIndex( master, testLabel, testProperty );

        try ( Transaction transaction = anySlave.beginTx() )
        {
            assertEquals( 1, Iterables.count( anySlave.schema().getIndexes( testLabel ) ) );
            transaction.success();
        }
        assertTrue( getRequestedLocks( anySlave ).isEmpty() );
    }

    private void createSingleTestLabeledNode( HighlyAvailableGraphDatabase master )
    {
        try ( Transaction tx = master.beginTx() )
        {
            master.createNode( testLabel );
            tx.success();
        }
    }

    private void createIndex( HighlyAvailableGraphDatabase master, Label label, String property )
    {
        try ( Transaction transaction = master.beginTx() )
        {
            master.schema().indexFor( label ).on( property ).create();
            transaction.success();
        }

        try ( Transaction transaction = master.beginTx() )
        {
            master.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            transaction.success();
        }
    }

    private List<LockRecord> getRequestedLocks( HighlyAvailableGraphDatabase master )
    {
        Tracers tracers = master.getDependencyResolver().resolveDependency( Tracers.class );
        RecordingLockTracer lockTracer = (RecordingLockTracer) tracers.lockTracer;
        return lockTracer.getRequestedLocks();
    }

}
