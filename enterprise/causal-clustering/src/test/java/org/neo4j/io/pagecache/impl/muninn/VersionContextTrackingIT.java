/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.core.Is.is;
import static org.neo4j.kernel.configuration.Settings.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class VersionContextTrackingIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();
    private static final int NUMBER_OF_TRANSACTIONS = 3;
    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.withSharedCoreParam( GraphDatabaseSettings.snapshot_query, TRUE )
                             .withSharedReadReplicaParam( GraphDatabaseSettings.snapshot_query, TRUE )
                             .startCluster();
    }

    @Test
    public void coreMemberTransactionIdPageTracking() throws Exception
    {
        long baseTxId = getBaseTransactionId();
        for ( int i = 1; i < 4; i++ )
        {
            generateData();
            long expectedLatestPageVersion = getExpectedLatestPageVersion( baseTxId, i );
            ThrowingSupplier<Long,Exception> anyCoreSupplier =
                    () -> getLatestPageVersion( getAnyCore() );
            assertEventually( "Any core page version should match to expected page version.", anyCoreSupplier,
                    is( expectedLatestPageVersion ), 2, MINUTES );
        }
    }

    @Test
    public void readReplicatesTransactionIdPageTracking() throws Exception
    {
        long baseTxId = getBaseTransactionId();
        for ( int i = 1; i < 4; i++ )
        {
            generateData();
            long expectedLatestPageVersion = getExpectedLatestPageVersion( baseTxId, i );
            ThrowingSupplier<Long,Exception> replicateVersionSupplier =
                    () -> getLatestPageVersion( getAnyReadReplica() );
            assertEventually( "Read replica page version should match to core page version.", replicateVersionSupplier,
                    is( expectedLatestPageVersion ), 2, MINUTES );
        }
    }

    private long getExpectedLatestPageVersion( long baseTxId, int round )
    {
        return baseTxId + round * NUMBER_OF_TRANSACTIONS;
    }

    private long getBaseTransactionId()
    {
        DependencyResolver dependencyResolver = getAnyCore().getDependencyResolver();
        TransactionIdStore transactionIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        return transactionIdStore.getLastClosedTransactionId();
    }

    private CoreClusterMember anyCoreClusterMember()
    {
        return cluster.coreMembers().iterator().next();
    }

    private CoreGraphDatabase getAnyCore()
    {
        return anyCoreClusterMember().database();
    }

    private ReadReplicaGraphDatabase getAnyReadReplica()
    {
        return cluster.findAnyReadReplica().database();
    }

    private static long getLatestPageVersion( GraphDatabaseFacade databaseFacade ) throws IOException
    {
        DependencyResolver dependencyResolver = databaseFacade.getDependencyResolver();
        PageCache pageCache = dependencyResolver.resolveDependency( PageCache.class );
        NeoStores neoStores = dependencyResolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        File storeFile = neoStores.getNodeStore().getStorageFileName();
        long maxTransactionId = Long.MIN_VALUE;
        try ( PagedFile pageFile = pageCache.getExistingMapping( storeFile ).get() )
        {
            long lastPageId = pageFile.getLastPageId();
            for ( int i = 0; i <= lastPageId; i++ )
            {
                try ( CursorPageAccessor pageCursor = new CursorPageAccessor(
                        (MuninnPageCursor) pageFile.io( i, PagedFile.PF_SHARED_READ_LOCK ) ) )
                {
                    if ( pageCursor.next() )
                    {
                        maxTransactionId = Math.max( maxTransactionId, pageCursor.lastTxModifierId() );
                    }
                }
            }
        }
        return maxTransactionId;
    }

    private void generateData() throws Exception
    {
        for ( int i = 0; i < NUMBER_OF_TRANSACTIONS; i++ )
        {
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                coreGraphDatabase.createNode();
                transaction.success();
            } );
        }
    }

    private static class CursorPageAccessor extends MuninnPageCursor
    {

        private MuninnPageCursor delegate;

        CursorPageAccessor( MuninnPageCursor delegate )
        {
            super( -1, PageCursorTracer.NULL, EmptyVersionContextSupplier.EMPTY );
            this.delegate = delegate;
        }

        long lastTxModifierId()
        {
            return delegate.pagedFile.getLastModifiedTxId( delegate.pinnedPageRef );
        }

        @Override
        protected void unpinCurrentPage()
        {
            delegate.unpinCurrentPage();
        }

        @Override
        protected void convertPageFaultLock( long pageRef )
        {
            delegate.convertPageFaultLock( pageRef );
        }

        @Override
        protected void pinCursorToPage( long pageRef, long filePageId, PageSwapper swapper )
                throws FileIsNotMappedException
        {
            delegate.pinCursorToPage( pageRef, filePageId, swapper );
        }

        @Override
        protected boolean tryLockPage( long pageRef )
        {
            return delegate.tryLockPage( pageRef );
        }

        @Override
        protected void unlockPage( long pageRef )
        {
            delegate.unlockPage( pageRef );
        }

        @Override
        public boolean next() throws IOException
        {
            return delegate.next();
        }

        @Override
        public boolean shouldRetry() throws IOException
        {
            return delegate.shouldRetry();
        }
    }
}
