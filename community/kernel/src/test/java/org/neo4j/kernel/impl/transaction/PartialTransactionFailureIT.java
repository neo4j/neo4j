/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.adversaries.ClassGuardedAdversary;
import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.api.scan.InMemoryLabelScanStoreExtension;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.cache.SoftCacheProvider;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Here we are verifying that even if we get an exception from the storage layer during commit,
 * we should still be able to recover to a consistent state.
 */
public class PartialTransactionFailureIT
{
    @Rule
    public TargetDirectory.TestDirectory dir =
            TargetDirectory.testDirForTest( PartialTransactionFailureIT.class );

    @Test
    public void concurrentlyCommittingTransactionsMustNotRotateOutLoggedCommandsOfFailingTransaction()
            throws Exception
    {
        final ClassGuardedAdversary adversary = new ClassGuardedAdversary(
                new CountingAdversary( 1, false ),
                "org.neo4j.kernel.impl.nioneo.xa.Command$RelationshipCommand" );
        adversary.disable();

        Map<String, String> params = stringMap(
                "logical_log_rotation_threshold", "1",
                "use_memory_mapped_buffers", "false");
        String storeDir = dir.directory().getAbsolutePath();
        final EmbeddedGraphDatabase db = new TestEmbeddedGraphDatabase( storeDir, params ) {
            @Override
            protected FileSystemAbstraction createFileSystemAbstraction()
            {
                return new AdversarialFileSystemAbstraction( adversary );
            }
        };


        Node a, b, c, d;
        Transaction tx = db.beginTx();
        try
        {
            a = db.createNode();
            b = db.createNode();
            c = db.createNode();
            d = db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        adversary.enable();
        CountDownLatch latch = new CountDownLatch( 1 );
        Thread t1 = new Thread( createRelationship( db, a, b, latch ) );
        Thread t2 = new Thread( createRelationship( db, c, d, latch ) );
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
        EmbeddedGraphDatabase db2 = new TestEmbeddedGraphDatabase( storeDir, stringMap() );
        tx = db2.beginTx();
        try
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
            try
            {
                tx.finish();
            }
            finally
            {
                db2.shutdown();
            }
        }
    }

    private Runnable createRelationship(
            final EmbeddedGraphDatabase db,
            final Node x,
            final Node y,
            final CountDownLatch latch )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                Transaction tx = db.beginTx();
                try
                {
                    x.createRelationshipTo( y, DynamicRelationshipType.withName( "r" ) );
                    tx.success();
                    latch.await();
                    tx.finish();
                }
                catch ( Exception ignore )
                {
                    // We don't care about our transactions failing, as long as we
                    // can recover our database to a consistent state.
                }
            }
        };
    }

    private static class TestEmbeddedGraphDatabase extends EmbeddedGraphDatabase
    {
        public TestEmbeddedGraphDatabase( String storeDir, Map<String, String> params )
        {
            super( storeDir,
                    params,
                    dependencies() );
        }

        private static Dependencies dependencies()
        {
            GraphDatabaseFactoryState state = new GraphDatabaseFactoryState();
            state.addKernelExtensions( Arrays.asList(
                    new InMemoryIndexProviderFactory(),
                    new InMemoryLabelScanStoreExtension() ) );
            state.setCacheProviders( Arrays.<CacheProvider>asList( new SoftCacheProvider() ) );
            state.setTransactionInterceptorProviders( Iterables.<TransactionInterceptorProvider>empty() );
            return state.databaseDependencies();
        }
    }
}
