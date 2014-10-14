/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.api.TxState;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.state.TxStateImpl;
import org.neo4j.kernel.impl.api.store.CacheLayer;
import org.neo4j.kernel.impl.api.store.DiskLayer;
import org.neo4j.kernel.impl.api.store.PersistenceCache;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.cache.AutoLoadingCache.Loader;
import org.neo4j.kernel.impl.cache.NoCache;
import org.neo4j.kernel.impl.cache.StrongReferenceCache;
import org.neo4j.kernel.impl.core.EntityFactory;
import org.neo4j.kernel.impl.core.LabelTokenHolder;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.core.RelationshipLoader;
import org.neo4j.kernel.impl.core.RelationshipTypeTokenHolder;
import org.neo4j.kernel.impl.core.TokenCreator;
import org.neo4j.kernel.impl.index.LegacyIndexStore;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.SchemaStorage;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.state.RecordAccessSet;
import org.neo4j.kernel.impl.transaction.state.RelationshipChainLoader;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreator;
import org.neo4j.kernel.impl.transaction.state.RelationshipLocker;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.kernel.impl.store.StoreFactory.configForStoreDir;
import static org.neo4j.kernel.impl.transaction.state.CacheLoaders.nodeLoader;
import static org.neo4j.kernel.impl.util.Providers.singletonProvider;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class NodeCommitAndReadRaceTest
{
    @Test
    public void shouldNotReadNodeFromStoreBeforeCommitterPutsItThere() throws Exception
    {
        /* Scenario comes from a time in 2.2 dev branch where we didn't put nodes in cache up front like we
         * used to do, instead the committer put it after store was updated. This allowed an independent reader
         * to come in and read from the store before the committer had put it in the cache making the cached
         * node receive duplicate relationships for the initial batch of relationships.
         *
         * Fix is to put a node reservation in cache when the user transaction creates the node, with the
         * sole purpose to communicate this fact to readers. The committer will then switch the reservation
         * for a normal node on commit. */

        // GIVEN
        // ... a created, but not yet fully committed, node
        KernelStatement creationStatement = mockedStatement();
        long nodeId = operations.nodeCreate( creationStatement );
        // ... and we pretend that this is during commit and so the node exists in the store
        createNodeWithSomeRelationships( nodeId );

        // WHEN a reader comes in and tries to read that node, it should not be able to see it
        KernelStatement readerStatement = mockedStatement();
        try
        {
            operations.nodeGetRelationships( readerStatement, nodeId, BOTH );
            fail( "Reader should not have seen this node" );
        }
        catch ( EntityNotFoundException e )
        {
            // THEN we're all good
        }
    }

    private void createNodeWithSomeRelationships( long nodeId )
    {
        RecordAccessSet recordAccess = new DirectRecordAccessSet( neoStore );
        NodeRecord node = recordAccess.getNodeChanges().create( nodeId, null ).forChangingData();
        node.setInUse( true );
        node.setCreated();

        int type = relationshipTokenHolder.getOrCreateId( "TYPE" );
        RelationshipCreator relationshipCreator = new RelationshipCreator( RelationshipLocker.NO_LOCKING, null, 100 );
        for ( int i = 0; i < 2; i++ )
        {
            relationshipCreator.relationshipCreate( neoStore.getRelationshipStore().nextId(),
                    type, nodeId, nodeId, recordAccess );
        }
        recordAccess.close();
    }

    private KernelStatement mockedStatement()
    {
        KernelStatement statement = mock( KernelStatement.class );
        TxState txState = new TxStateImpl( mock( LegacyIndexTransactionState.class ) );
        when( statement.txState() ).thenReturn( txState );
        return statement;
    }

    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    public final @Rule PageCacheRule pageCacheRule = new PageCacheRule();
    private StateHandlingStatementOperations operations;
    private NeoStore neoStore;
    private RelationshipTypeTokenHolder relationshipTokenHolder;

    @Before
    @SuppressWarnings( { "unchecked", "deprecation" } )
    public void before()
    {
        File storeDir = new File( "dir" );
        Config config = configForStoreDir( new Config(), storeDir );
        PageCache pageCache = pageCacheRule.getPageCache( fsr.get(), config );
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        Monitors monitors = new Monitors();
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, pageCache, fsr.get(),
                DEV_NULL, monitors );
        neoStore = storeFactory.newNeoStore( true );
        IndexingService indexingService = mock( IndexingService.class );
        PropertyKeyTokenHolder propertyKeyTokenHolder = new PropertyKeyTokenHolder( mock( TokenCreator.class ) );
        LabelTokenHolder labelTokenHolder = new LabelTokenHolder( mock( TokenCreator.class ) );
        TokenCreator relationshipTypeTokenCreator = new DirectTokenCreator( neoStore );
        relationshipTokenHolder =
                new RelationshipTypeTokenHolder( relationshipTypeTokenCreator );
        DiskLayer diskLayer = new DiskLayer( propertyKeyTokenHolder, labelTokenHolder, relationshipTokenHolder,
                mock( SchemaStorage.class ), singletonProvider( neoStore ), indexingService );
        Loader<NodeImpl> loader = nodeLoader( neoStore.getNodeStore() );
        AutoLoadingCache<NodeImpl> nodeCache = new AutoLoadingCache<>(
                new StrongReferenceCache<NodeImpl>( getClass().getSimpleName() ), loader );
        EntityFactory entityFactory = mock( EntityFactory.class );
        RelationshipChainLoader relationshipChainLoader = new RelationshipChainLoader( neoStore );
        RelationshipLoader relationshipLoader = new RelationshipLoader(
                LockService.NO_LOCK_SERVICE, new NoCache<RelationshipImpl>( getClass().getSimpleName() ), relationshipChainLoader );
        PersistenceCache cache = /*spy( */new PersistenceCache( nodeCache, mock( AutoLoadingCache.class ),
                entityFactory, relationshipLoader, propertyKeyTokenHolder, relationshipTokenHolder, labelTokenHolder,
                null) /*)*/;
        // Just an assertion that this apply method isn't called.
//        doThrow( new IllegalStateException( "This method should not be called" ) )
//                .when( cache ).apply( any( TxState.class ) );
        StoreReadLayer readLayer = new CacheLayer( diskLayer, cache, indexingService, mock( SchemaCache.class ) );
        operations = new StateHandlingStatementOperations( readLayer,
                mock( LegacyPropertyTrackers.class ), mock( ConstraintIndexCreator.class ),
                mock( LegacyIndexStore.class ) );
    }

    @After
    public void after()
    {
        neoStore.close();
    }
}
