/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import javax.annotation.Resource;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.internal.EmbeddedGraphDatabase;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Here we are verifying that even if we get an exception from the storage layer during commit,
 * we should still be able to recover to a consistent state.
 */
@ExtendWith( TestDirectoryExtension.class )
public class PartialTransactionFailureIT
{
    @Resource
    public TestDirectory dir;

    @Test
    public void concurrentlyCommittingTransactionsMustNotRotateOutLoggedCommandsOfFailingTransaction()
            throws Exception
    {
        final ClassGuardedAdversary adversary = new ClassGuardedAdversary(
                new CountingAdversary( 1, false ),
                Command.RelationshipCommand.class );
        adversary.disable();

        File storeDir = dir.graphDbDir();
        final Map<String,String> params = stringMap( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        final EmbeddedGraphDatabase db = new TestEmbeddedGraphDatabase( storeDir, params )
        {
            @Override
            protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
            {
                new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
                {
                    @Override
                    protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                            GraphDatabaseFacade graphDatabaseFacade )
                    {
                        return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                        {
                            @Override
                            protected FileSystemAbstraction createFileSystemAbstraction()
                            {
                                return new AdversarialFileSystemAbstraction( adversary );
                            }
                        };
                    }
                }.initFacade( storeDir, params, dependencies, this );
            }
        };

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
            tx.success();
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
        db.shutdown();

        // We should observe the store in a consistent state
        EmbeddedGraphDatabase db2 = new TestEmbeddedGraphDatabase( storeDir, params );
        try ( Transaction tx = db2.beginTx() )
        {
            Node x = db2.getNodeById( a.getId() );
            Node y = db2.getNodeById( b.getId() );
            Node z = db2.getNodeById( c.getId() );
            Node w = db2.getNodeById( d.getId() );
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
        finally
        {
            db2.shutdown();
        }
    }

    private Runnable createRelationship(
            final EmbeddedGraphDatabase db,
            final Node x,
            final Node y,
            final CountDownLatch latch )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                x.createRelationshipTo( y, RelationshipType.withName( "r" ) );
                tx.success();
                latch.await();
                db.getDependencyResolver().resolveDependency( LogRotation.class ).rotateLogFile();
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

    private static class TestEmbeddedGraphDatabase extends EmbeddedGraphDatabase
    {
        TestEmbeddedGraphDatabase( File storeDir, Map<String,String> params )
        {
            super( storeDir,
                    params,
                    dependencies() );
        }

        private static GraphDatabaseFacadeFactory.Dependencies dependencies()
        {
            GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
            return state.databaseDependencies();
        }
    }
}
