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
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContext;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class VersionContextTrackingIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );
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
            dataMatchesEventually( anyCoreClusterMember(), cluster.coreMembers() );
            assertEquals( getExpectedLatestPageVersion( baseTxId, i ), getLatestPageVersion( getAnyCore() ) );
        }
    }

    @Test
    public void readReplicateTransactionIdPageTracking() throws Exception
    {
        long baseTxId = getBaseTransactionId();
        for ( int i = 1; i < 4; i++ )
        {
            generateData();
            dataMatchesEventually( anyCoreClusterMember(), cluster.readReplicas() );
            assertEquals( getExpectedLatestPageVersion( baseTxId, i ), getLatestPageVersion( getAnyReadReplica() ) );
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

    private long getLatestPageVersion( GraphDatabaseFacade databaseFacade ) throws IOException
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

    private class CursorPageAccessor extends MuninnPageCursor
    {

        private MuninnPageCursor delegate;

        CursorPageAccessor( MuninnPageCursor delegate )
        {
            super( -1, PageCursorTracer.NULL, EmptyVersionContext.INSTANCE );
            this.delegate = delegate;
        }

        long lastTxModifierId()
        {
            return delegate.page.getLastModifiedTxId();
        }

        @Override
        protected void unpinCurrentPage()
        {
            delegate.unpinCurrentPage();
        }

        @Override
        protected void convertPageFaultLock( MuninnPage page )
        {
            delegate.convertPageFaultLock( page );
        }

        @Override
        protected void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper )
        {
            delegate.pinCursorToPage( page, filePageId, swapper );
        }

        @Override
        protected boolean tryLockPage( MuninnPage page )
        {
            return delegate.tryLockPage( page );
        }

        @Override
        protected void unlockPage( MuninnPage page )
        {
            delegate.unlockPage( page );
        }

        @Override
        protected void releaseCursor()
        {
            delegate.releaseCursor();
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
