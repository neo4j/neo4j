/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

/**
 * Here we are verifying that even if we get an exception from the storage layer during commit,
 * we should still be able to recover to a consistent state.
 */
@TestDirectoryExtension
class PartialTransactionFailureIT
{
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void concurrentlyCommittingTransactionsMustNotRotateOutLoggedCommandsOfFailingTransaction() throws Exception
    {
        final ClassGuardedAdversary adversary = new ClassGuardedAdversary(
                new CountingAdversary( 1, false ),
                Command.RelationshipCommand.class );
        adversary.disable();

        File storeDir = testDirectory.storeDir();
        final Map<Setting<?>,Object> params = Map.of( GraphDatabaseSettings.pagecache_memory, "8m" );
        managementService = new TestDatabaseManagementServiceBuilder( storeDir )
                .setFileSystem( new AdversarialFileSystemAbstraction( adversary ) )
                .setConfig( params )
                .build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        Node a;
        Node b;
        Node c;
        Node d;
        try ( Transaction tx = db.beginTx() )
        {
            a = db.createNode();
            b = db.createNode();
            c = db.createNode();
            d = db.createNode();
            tx.commit();
        }

        adversary.enable();
        CountDownLatch latch = new CountDownLatch( 1 );
        Thread t1 = new Thread( createRelationship( db, a, b, latch ), "T1" );
        Thread t2 = new Thread( createRelationship( db, c, d, latch ), "T2" );
        t1.start();
        t2.start();
        // Wait for both threads to get going
        t1.join( 10 );
        t2.join( 10 );
        latch.countDown();

        // Wait for the transactions to finish
        t1.join( 25000 );
        t2.join( 25000 );
        managementService.shutdown();

        // We should observe the store in a consistent state
        managementService = new TestDatabaseManagementServiceBuilder( storeDir ).setConfig( params ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = database.beginTx() )
        {
            Node x = database.getNodeById( a.getId() );
            Node y = database.getNodeById( b.getId() );
            Node z = database.getNodeById( c.getId() );
            Node w = database.getNodeById( d.getId() );
            Iterator<Relationship> itrRelX = x.getRelationships().iterator();
            Iterator<Relationship> itrRelY = y.getRelationships().iterator();
            Iterator<Relationship> itrRelZ = z.getRelationships().iterator();
            Iterator<Relationship> itrRelW = w.getRelationships().iterator();

            if ( itrRelX.hasNext() != itrRelY.hasNext() )
            {
                fail( "Node x and y have inconsistent relationship counts" );
            }
            else if ( itrRelX.hasNext() )
            {
                Relationship rel = itrRelX.next();
                assertEquals( rel, itrRelY.next() );
                assertFalse( itrRelX.hasNext() );
                assertFalse( itrRelY.hasNext() );
            }

            if ( itrRelZ.hasNext() != itrRelW.hasNext() )
            {
                fail( "Node z and w have inconsistent relationship counts" );
            }
            else if ( itrRelZ.hasNext() )
            {
                Relationship rel = itrRelZ.next();
                assertEquals( rel, itrRelW.next() );
                assertFalse( itrRelZ.hasNext() );
                assertFalse( itrRelW.hasNext() );
            }
        }
    }

    private static Runnable createRelationship( GraphDatabaseAPI db, final Node x, final Node y, final CountDownLatch latch )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                x.createRelationshipTo( y, RelationshipType.withName( "r" ) );
                tx.commit();
                latch.await();
                db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile( LogAppendEvent.NULL );
                db.getDependencyResolver().resolveDependency( CheckPointer.class ).forceCheckPoint(
                        new SimpleTriggerInfo( "test" )
                );
            }
            catch ( Exception ignore )
            {
                // We don't care about our transactions failing, as long as we
                // can recover our database to a consistent state.
            }
        };
    }
}
