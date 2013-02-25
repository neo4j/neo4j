/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.SchemaIndexProvider.NO_INDEX_PROVIDER;
import static org.neo4j.kernel.impl.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.impl.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.impl.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.impl.util.StringLogger.SYSTEM;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class WriteTransactionTest
{
    @Test
    public void shouldAddSchemaRuleToCacheWhenApplyingTransactionThatCreatesOne() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );

        // WHEN
        final long ruleId = neoStore.getSchemaStore().nextId();
        IndexRule schemaRule = new IndexRule( ruleId, 10, 8 );
        writeTransaction.createSchemaRule( schemaRule );
        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        verify( cacheAccessBackDoor ).addSchemaRule( schemaRule );
    }

    @Test
    public void shouldRemoveSchemaRuleFromCacheWhenApplyingTransactionThatDeletesOne() throws Exception
    {
        // GIVEN
        SchemaStore schemaStore = neoStore.getSchemaStore();
        long labelId = 10, propertyKey = 10;
        IndexRule rule = new IndexRule( schemaStore.nextId(), labelId, propertyKey );
        Collection<DynamicRecord> records = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : records )
            schemaStore.updateRecord( record );
        long ruleId = first( records ).getId();
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );

        // WHEN
        writeTransaction.dropSchemaRule( ruleId );
        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        verify( cacheAccessBackDoor ).removeSchemaRuleFromCache( ruleId );
    }
    
    @Test
    public void shouldRemoveSchemaRuleWhenRollingBackTransaction() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );

        // WHEN
        final long ruleId = neoStore.getSchemaStore().nextId();
        writeTransaction.createSchemaRule( new IndexRule( ruleId, 10, 7 ) );
        writeTransaction.prepare();
        writeTransaction.rollback();

        // THEN
        verifyNoMoreInteractions( cacheAccessBackDoor );
    }
    
    @Test
    public void shouldWriteProperBeforeAndAfterPropertyRecordsWhenAddingProperty() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        int nodeId = 1;
        writeTransaction.setCommitTxId( nodeId );
        writeTransaction.nodeCreate( nodeId );
        PropertyIndex propertyIndex = new PropertyIndex( "key", 1 );
        Object value = 5;

        // WHEN
        writeTransaction.nodeAddProperty( nodeId, propertyIndex, value );
        writeTransaction.doPrepare();

        // THEN
        PropertyCommand propertyCommand = (PropertyCommand) single( filter( new Predicate<XaCommand>()
        {
            @Override
            public boolean accept( XaCommand item )
            {
                return item instanceof PropertyCommand;
            }
        }, log.commands ) );
        PropertyRecord before = propertyCommand.getBefore();
        assertFalse( before.inUse() );
        assertEquals( Collections.emptyList(), before.getPropertyBlocks() );

        PropertyRecord after = propertyCommand.getAfter();
        assertTrue( after.inUse() );
        assertEquals( 1, count( after.getPropertyBlocks() ) );
    }
    
    // TODO change property record
    // TODO remove property record
    
    @Test
    public void shouldConvertAddedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1;
        CapturingIndexingService indexingService = new CapturingIndexingService();
        WriteTransaction writeTransaction = newWriteTransaction( indexingService );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        
        // WHEN
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyIndex1.getKeyId(), value1, none ),
                add( nodeId, propertyIndex2.getKeyId(), value2, none ) ),
                
                indexingService.updates );
    }
    
    @Test
    public void shouldConvertChangedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 1;
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        PropertyData property1 = writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        PropertyData property2 = writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        Object newValue1 = "new", newValue2 = "new 2";
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeChangeProperty( nodeId, property1, newValue1 );
        writeTransaction.nodeChangeProperty( nodeId, property2, newValue2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                change( nodeId, propertyIndex1.getKeyId(), value1, none, newValue1, none ),
                change( nodeId, propertyIndex2.getKeyId(), value2, none, newValue2, none ) ),
                
                indexingService.updates );
    }
    
    @Test
    public void shouldConvertRemovedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 1;
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        PropertyData property1 = writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        PropertyData property2 = writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeRemoveProperty( nodeId, property1 );
        writeTransaction.nodeRemoveProperty( nodeId, property2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyIndex1.getKeyId(), value1, none ),
                remove( nodeId, propertyIndex2.getKeyId(), value2, none ) ),
                
                indexingService.updates );
    }

    @Test
    public void shouldConvertLabelAdditionToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1, labelId = 3;
        long[] labelIds = new long[] {labelId};
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.addLabelToNode( labelId, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyIndex1.getKeyId(), value1, labelIds ),
                add( nodeId, propertyIndex2.getKeyId(), value2, labelIds ) ),
                
                indexingService.updates );
    }

    @Test
    public void shouldConvertMixedLabelAdditionAndSetPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1, labelId1 = 3, labelId2 = 4;
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyIndex1.getKeyId(), value1, new long[] {labelId2} ),
                add( nodeId, propertyIndex2.getKeyId(), value2, new long[] {labelId2} ),
                add( nodeId, propertyIndex2.getKeyId(), value2, new long[] {labelId1, labelId2} ) ),
                
                indexingService.updates );
    }
    
    @Test
    public void shouldConvertLabelRemovalToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1, labelId = 3;
        long[] labelIds = new long[] {labelId};
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.addLabelToNode( labelId, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.removeLabelFromNode( labelId, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyIndex1.getKeyId(), value1, labelIds ),
                remove( nodeId, propertyIndex2.getKeyId(), value2, labelIds ) ),
                
                indexingService.updates );
    }
    
    @Test
    public void shouldConvertMixedLabelRemovalAndRemovePropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1, labelId1 = 3, labelId2 = 4;
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        PropertyData property1 = writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();
        
        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeRemoveProperty( nodeId, property1 );
        writeTransaction.removeLabelFromNode( labelId2, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyIndex1.getKeyId(), value1, new long[] {labelId1, labelId2} ),
                remove( nodeId, propertyIndex2.getKeyId(), value2, new long[] {labelId2} ) ),
                
                indexingService.updates );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndAddPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 1, labelId1 = 3, labelId2 = 4;
        WriteTransaction writeTransaction = newWriteTransaction( NO_INDEXING );
        PropertyIndex propertyIndex1 = new PropertyIndex( "key", 1 ), propertyIndex2 = new PropertyIndex( "key2", 2 );
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        PropertyData property1 = writeTransaction.nodeAddProperty( nodeId, propertyIndex1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeAddProperty( nodeId, propertyIndex2, value2 );
        writeTransaction.removeLabelFromNode( labelId2, nodeId );
        writeTransaction.doPrepare();
        writeTransaction.doCommit();

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyIndex2.getKeyId(), value2, new long[] {labelId1} ),
                remove( nodeId, propertyIndex1.getKeyId(), value1, new long[] {labelId2} ),
                remove( nodeId, propertyIndex2.getKeyId(), value2, new long[] {labelId2} ) ),

                indexingService.updates );
    }
    
    @Test
    public void shouldUpdateHighIdsOnRecoveredTransaction() throws Exception
    {
        // GIVEN
        WriteTransaction tx = newWriteTransaction( NO_INDEXING );
        int nodeId = 5, relId = 10, relationshipType = 3, propertyIndexId = 4, ruleId = 8;
        PropertyIndex propertyIndex = new PropertyIndex( "key", propertyIndexId );

        // WHEN
        tx.setRecovered();
        tx.nodeCreate( nodeId );
        tx.createRelationshipType( relationshipType, "type" );
        tx.relationshipCreate( relId, 0, nodeId, nodeId );
        tx.relAddProperty( relId, propertyIndex,
                new long[] {1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60, 1 << 60} );
        tx.createPropertyIndex( propertyIndex.getKey(), propertyIndex.getKeyId() );
        tx.nodeAddProperty( nodeId, propertyIndex,
                "something long and nasty that requires dynamic records for sure I would think and hope. Ok then åäö%!=" );
        for ( int i = 0; i < 10; i++ )
            tx.addLabelToNode( 10000 + i, nodeId );
        tx.createSchemaRule( new IndexRule( ruleId, 100, propertyIndexId ) );
        tx.doPrepare();
        tx.doCommit();

        // THEN
        assertEquals( "NodeStore", nodeId+1, neoStore.getNodeStore().getHighId() );
        assertEquals( "DynamicNodeLabelStore", 2, neoStore.getNodeStore().getDynamicLabelStore().getHighId() );
        assertEquals( "RelationshipStore", relId+1, neoStore.getRelationshipStore().getHighId() );
        assertEquals( "RelationshipTypeStore", relationshipType+1, neoStore.getRelationshipTypeStore().getHighId() );
        assertEquals( "RelationshipType NameStore", 2, neoStore.getRelationshipTypeStore().getNameStore().getHighId() );
        assertEquals( "PropertyStore", 2, neoStore.getPropertyStore().getHighId() );
        assertEquals( "PropertyStore DynamicStringStore", 2, neoStore.getPropertyStore().getStringStore().getHighId() );
        assertEquals( "PropertyStore DynamicArrayStore", 2, neoStore.getPropertyStore().getArrayStore().getHighId() );
        assertEquals( "PropertyIndexStore", propertyIndexId+1, neoStore.getPropertyStore().getIndexStore().getHighId() );
        assertEquals( "PropertyIndex NameStore", 2, neoStore.getPropertyStore().getIndexStore().getNameStore().getHighId() );
        assertEquals( "SchemaStore", ruleId+1, neoStore.getSchemaStore().getHighId() );
    }

    private EphemeralFileSystemAbstraction fileSystemAbstraction;
    private CapturingXaLogicalLog log;
    private TransactionState transactionState;
    private final Config config = new Config( stringMap() );
    private final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private final DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
    private StoreFactory storeFactory;
    private NeoStore neoStore;
    private CacheAccessBackDoor cacheAccessBackDoor;
    
    @Before
    public void before() throws Exception
    {
        fileSystemAbstraction = new EphemeralFileSystemAbstraction();
        log = new CapturingXaLogicalLog( fileSystemAbstraction );
        transactionState = TransactionState.NO_STATE;
        storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, SYSTEM, new DefaultTxHook() );
        neoStore = storeFactory.createNeoStore( new File( "neostore" ) );
        cacheAccessBackDoor = mock( CacheAccessBackDoor.class );
    }
    
    private static class CapturingXaLogicalLog extends XaLogicalLog
    {
        private final Collection<XaCommand> commands = new ArrayList<XaCommand>();

        public CapturingXaLogicalLog( FileSystemAbstraction fs )
        {
            super( new File( "log" ), null, null, null, new DefaultLogBufferFactory(),
                    fs, new SingleLoggingService( SYSTEM ), LogPruneStrategies.NO_PRUNING, null );
        }
        
        @Override
        public synchronized void writeCommand( XaCommand command, int identifier ) throws IOException
        {
            commands.add( command );
        }
    }
    
    @After
    public void after() throws Exception
    {
        fileSystemAbstraction.shutdown();
    }
    
    private ArgumentMatcher<Collection<DynamicRecord>> firstRecordIdMatching( final int ruleId )
    {
        return new ArgumentMatcher<Collection<DynamicRecord>>()
        {
            @Override
            public boolean matches( Object argument )
            {
                Collection<DynamicRecord> records = (Collection<DynamicRecord>) argument; 
                return first( records ).getId() == ruleId;
            }
        };
    }

    static IndexingService NO_INDEXING = mock( IndexingService.class );

    private WriteTransaction newWriteTransaction( IndexingService indexing )
    {
        WriteTransaction result = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, indexing );
        result.setCommitTxId( neoStore.getLastCommittedTx()+1 );
        return result;
    }
    
    private class CapturingIndexingService extends IndexingService
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<NodePropertyUpdate>();
        
        public CapturingIndexingService()
        {
            super( null, NO_INDEX_PROVIDER, new NeoStoreIndexStoreView( neoStore ), new SingleLoggingService( SYSTEM ) );
        }
        
        @Override
        public void update( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }
    }
    
    private static final long[] none = new long[0];
}
