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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.api.KernelTransactionImplementation;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.scan.LabelScanReader;
import org.neo4j.kernel.api.scan.LabelScanStore;
import org.neo4j.kernel.api.scan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.NO_INDEX_PROVIDER;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.indexRule;
import static org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule.uniquenessConstraintRule;
import static org.neo4j.kernel.impl.transaction.xaframework.InjectedTransactionValidator.ALLOW_ALL;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;
import static org.neo4j.test.AllItemsMatcher.matchesAll;

public class WriteTransactionTest
{
    public static final String LONG_STRING = "string value long enough not to be stored as a short string";

    @Test
    public void shouldValidateConstraintIndexAsPartOfPrepare() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );

        final long indexId = neoStore.getSchemaStore().nextId();
        final long constraintId = neoStore.getSchemaStore().nextId();

        writeTransaction.createSchemaRule( uniquenessConstraintRule( constraintId, 1, 1, indexId ) );

        // WHEN
        writeTransaction.prepare();

        // THEN
        verify( mockIndexing ).validateIndex( indexId );
    }

    @Test
    public void shouldAddSchemaRuleToCacheWhenApplyingTransactionThatCreatesOne() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );

        // WHEN
        final long ruleId = neoStore.getSchemaStore().nextId();
        IndexRule schemaRule = indexRule( ruleId, 10, 8, PROVIDER_DESCRIPTOR );
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
        int labelId = 10, propertyKey = 10;
        IndexRule rule = indexRule( schemaStore.nextId(), labelId, propertyKey, PROVIDER_DESCRIPTOR );
        Collection<DynamicRecord> records = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : records )
        {
            schemaStore.updateRecord( record );
        }
        long ruleId = first( records ).getId();
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );

        // WHEN
        writeTransaction.dropSchemaRule( rule );
        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        verify( cacheAccessBackDoor ).removeSchemaRuleFromCache( ruleId );
    }

    @Test
    public void shouldRemoveSchemaRuleWhenRollingBackTransaction() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );

        // WHEN
        final long ruleId = neoStore.getSchemaStore().nextId();
        writeTransaction.createSchemaRule( indexRule( ruleId, 10, 7, PROVIDER_DESCRIPTOR ) );
        writeTransaction.prepare();
        writeTransaction.rollback();

        // THEN
        verifyNoMoreInteractions( cacheAccessBackDoor );
    }

    @Test
    public void shouldWriteProperBeforeAndAfterPropertyRecordsWhenAddingProperty() throws Exception
    {
        // THEN
        Visitor<XaCommand, RuntimeException> verifier = new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element )
            {
                if ( element instanceof PropertyCommand )
                {
                    PropertyRecord before = ((PropertyCommand) element).getBefore();
                    assertFalse( before.inUse() );
                    assertEquals( Collections.<PropertyBlock>emptyList(), before.getPropertyBlocks() );

                    PropertyRecord after = ((PropertyCommand) element).getAfter();
                    assertTrue( after.inUse() );
                    assertEquals( 1, count( after.getPropertyBlocks() ) );
                }
                return true;
            }
        };

        // GIVEN
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing, verifier );
        int nodeId = 1;
        writeTransaction.setCommitTxId( nodeId );
        writeTransaction.nodeCreate( nodeId );
        int propertyKey = 1;
        Object value = 5;

        // WHEN
        writeTransaction.nodeAddProperty( nodeId, propertyKey, value );
        writeTransaction.doPrepare();
    }

    // TODO change property record
    // TODO remove property record

    @Test
    public void shouldConvertAddedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        CapturingIndexingService indexingService = new CapturingIndexingService();
        WriteTransaction writeTransaction = newWriteTransaction( indexingService );
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;

        // WHEN
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyKey1, value1, none ),
                add( nodeId, propertyKey2, value2, none ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertChangedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        DefinedProperty property1 = writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        Object newValue1 = "new", newValue2 = "new 2";
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeChangeProperty( nodeId, property1.propertyKeyId(), newValue1 );
        writeTransaction.nodeChangeProperty( nodeId, property2.propertyKeyId(), newValue2 );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                change( nodeId, propertyKey1, value1, none, newValue1, none ),
                change( nodeId, propertyKey2, value2, none, newValue2, none ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertRemovedPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        int nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        DefinedProperty property1 = writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeRemoveProperty( nodeId, property1.propertyKeyId() );
        writeTransaction.nodeRemoveProperty( nodeId, property2.propertyKeyId() );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyKey1, value1, none ),
                remove( nodeId, propertyKey2, value2, none ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertLabelAdditionToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2, labelId = 3;
        long[] labelIds = new long[] {labelId};
        Object value1 = LONG_STRING, value2 = LONG_STRING.getBytes();
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.addLabelToNode( labelId, nodeId );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyKey1, value1, labelIds ),
                add( nodeId, propertyKey2, value2, labelIds ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertMixedLabelAdditionAndSetPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyKey1, value1, new long[] {labelId2} ),
                add( nodeId, propertyKey2, value2, new long[]{labelId2} ),
                add( nodeId, propertyKey2, value2, new long[]{labelId1, labelId2} ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertLabelRemovalToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2, labelId = 3;
        long[] labelIds = new long[] {labelId};
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        writeTransaction.addLabelToNode( labelId, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.removeLabelFromNode( labelId, nodeId );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyKey1, value1, labelIds ),
                remove( nodeId, propertyKey2, value2, labelIds ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndRemovePropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        DefinedProperty property1 = writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeRemoveProperty( nodeId, property1.propertyKeyId() );
        writeTransaction.removeLabelFromNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                remove( nodeId, propertyKey1, value1, new long[] {labelId1, labelId2} ),
                remove( nodeId, propertyKey2, value2, new long[] {labelId2} ) ),

                indexingService.updates );
    }

    @Test
    public void shouldConvertMixedLabelRemovalAndAddPropertyToNodePropertyUpdates() throws Exception
    {
        // GIVEN
        long nodeId = 0;
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        writeTransaction.removeLabelFromNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // THEN
        assertEquals( asSet(
                add( nodeId, propertyKey2, value2, new long[]{labelId1} ),
                remove( nodeId, propertyKey1, value1, new long[]{labelId2} ),
                remove( nodeId, propertyKey2, value2, new long[]{labelId2} ) ),

                indexingService.updates );
    }

    @Test
    public void shouldUpdateHighIdsOnRecoveredTransaction() throws Exception
    {
        // GIVEN
        WriteTransaction tx = newWriteTransaction( mockIndexing );
        int nodeId = 5, relId = 10, relationshipType = 3, propertyKeyId = 4, ruleId = 8;

        // WHEN
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( relationshipType, "type" );
        tx.relationshipCreate( relId, 0, nodeId, nodeId );
        tx.relAddProperty( relId, propertyKeyId,
                new long[] {1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60, 1l << 60} );
        tx.createPropertyKeyToken( "key", propertyKeyId );
        tx.nodeAddProperty( nodeId, propertyKeyId,
                "something long and nasty that requires dynamic records for sure I would think and hope. Ok then åäö%!=" );
        for ( int i = 0; i < 10; i++ )
        {
            tx.addLabelToNode( 10000 + i, nodeId );
        }
        tx.createSchemaRule( indexRule( ruleId, 100, propertyKeyId, PROVIDER_DESCRIPTOR ) );
        prepareAndCommitRecovered( tx );

        // THEN
        assertEquals( "NodeStore", nodeId+1, neoStore.getNodeStore().getHighId() );
        assertEquals( "DynamicNodeLabelStore", 2, neoStore.getNodeStore().getDynamicLabelStore().getHighId() );
        assertEquals( "RelationshipStore", relId+1, neoStore.getRelationshipStore().getHighId() );
        assertEquals( "RelationshipTypeStore", relationshipType+1, neoStore.getRelationshipTypeStore().getHighId() );
        assertEquals( "RelationshipType NameStore", 2, neoStore.getRelationshipTypeStore().getNameStore().getHighId() );
        assertEquals( "PropertyStore", 2, neoStore.getPropertyStore().getHighId() );
        assertEquals( "PropertyStore DynamicStringStore", 2, neoStore.getPropertyStore().getStringStore().getHighId() );
        assertEquals( "PropertyStore DynamicArrayStore", 2, neoStore.getPropertyStore().getArrayStore().getHighId() );
        assertEquals( "PropertyIndexStore", propertyKeyId+1, neoStore.getPropertyStore().getPropertyKeyTokenStore().getHighId() );
        assertEquals( "PropertyKeyToken NameStore", 2, neoStore.getPropertyStore().getPropertyKeyTokenStore().getNameStore().getHighId() );
        assertEquals( "SchemaStore", ruleId+1, neoStore.getSchemaStore().getHighId() );
    }

    @Test
    public void createdSchemaRuleRecordMustBeWrittenHeavy() throws Exception
    {
        // THEN
        Visitor<XaCommand, RuntimeException> verifier = heavySchemaRuleVerifier();

        // GIVEN
        WriteTransaction tx = newWriteTransaction( mockIndexing, verifier );
        long ruleId = 0;
        int labelId = 5, propertyKeyId = 7;
        SchemaRule rule = indexRule( ruleId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );

        // WHEN
        tx.createSchemaRule( rule );
        prepareAndCommit( tx );
    }

    @Test
    public void shouldWriteProperPropertyRecordsWhenOnlyChangingLinkage() throws Exception
    {
        /* There was an issue where GIVEN:
         *
         *   Legend: () = node, [] = property record
         *
         *   ()-->[0:block{size:1}]
         *
         * WHEN adding a new property record in front of if, not changing any data in that record i.e:
         *
         *   ()-->[1:block{size:4}]-->[0:block{size:1}]
         *
         * The state of property record 0 would be that it had loaded value records for that block,
         * but those value records weren't heavy, so writing that record to the log would fail
         * w/ an assertion data != null.
         */

        // GIVEN
        WriteTransaction tx = newWriteTransaction( mockIndexing );
        int nodeId = 0;
        tx.nodeCreate( nodeId );
        int index = 0;
        tx.nodeAddProperty( nodeId, index, string( 70 ) ); // will require a block of size 1
        prepareAndCommit( tx );

        // WHEN
        Visitor<XaCommand, RuntimeException> verifier = new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element )
            {
                if ( element instanceof PropertyCommand )
                {
                    // THEN
                    PropertyCommand propertyCommand = (PropertyCommand) element;
                    verifyPropertyRecord( propertyCommand.getBefore() );
                    verifyPropertyRecord( propertyCommand.getAfter() );
                    return true;
                }
                return false;
            }

            private void verifyPropertyRecord( PropertyRecord record )
            {
                if ( record.getPrevProp() != Record.NO_NEXT_PROPERTY.intValue() )
                {
                    for ( PropertyBlock block : record.getPropertyBlocks() )
                    {
                        assertTrue( block.isLight() );
                    }
                }
            }
        };
        tx = newWriteTransaction( mockIndexing, verifier );
        int index2 = 1;
        tx.nodeAddProperty( nodeId, index2, string( 40 ) ); // will require a block of size 4
        prepareAndCommit( tx );
    }

    @Test
    public void shouldCreateEqualNodePropertyUpdatesOnRecoveryOfCreatedNode() throws Exception
    {
        /* There was an issue where recovering a tx where a node with a label and a property
         * was created resulted in two exact copies of NodePropertyUpdates. */

        // GIVEN
        long nodeId = 0;
        int labelId = 5, propertyKeyId = 7;
        NodePropertyUpdate expectedUpdate = NodePropertyUpdate.add( nodeId, propertyKeyId, "Neo", new long[] {labelId} );

        // -- an index
        long ruleId = 0;
        WriteTransaction tx = newWriteTransaction( mockIndexing );
        SchemaRule rule = indexRule( ruleId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        tx.createSchemaRule( rule );
        prepareAndCommit( tx );

        // -- and a tx creating a node with that label and property key
        IndexingService index = mock( IndexingService.class );
        CommandCapturingVisitor commandCapturingVisitor = new CommandCapturingVisitor();
        tx = newWriteTransaction( index, commandCapturingVisitor );
        tx.nodeCreate( nodeId );
        tx.addLabelToNode( labelId, nodeId );
        tx.nodeAddProperty( nodeId, propertyKeyId, "Neo" );
        prepareAndCommit( tx );
        verify( index, times( 1 ) ).updateIndexes( argThat( matchesAll( expectedUpdate ) ) );
        reset( index );

        // WHEN
        // -- later recovering that tx, there should be only one update
        tx = newWriteTransaction( index );
        commandCapturingVisitor.injectInto( tx );
        prepareAndCommitRecovered( tx );
        verify( index, times( 1 ) ).updateIndexes( argThat( matchesAll( expectedUpdate ) ) );
    }

    private String string( int length )
    {
        StringBuilder result = new StringBuilder();
        char ch = 'a';
        for ( int i = 0; i < length; i++ )
        {
            result.append( (char)((ch + (i%10))) );
        }
        return result.toString();
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private TransactionState transactionState;
    private final Config config = new Config( stringMap() );
    private final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private final DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
    private NeoStore neoStore;
    private CacheAccessBackDoor cacheAccessBackDoor;

    @Before
    public void before() throws Exception
    {
        transactionState = TransactionState.NO_STATE;
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory,
                fs.get(), DEV_NULL, new DefaultTxHook() );
        neoStore = storeFactory.createNeoStore( new File( "neostore" ) );
        cacheAccessBackDoor = mock( CacheAccessBackDoor.class );
    }

    private static class VerifyingXaLogicalLog extends XaLogicalLog
    {
        private final Visitor<XaCommand, RuntimeException> verifier;

        public VerifyingXaLogicalLog( FileSystemAbstraction fs, Visitor<XaCommand, RuntimeException> verifier )
        {
            super( new File( "log" ), null, null, null, new DefaultLogBufferFactory(),
                    fs, new SingleLoggingService( DEV_NULL ), LogPruneStrategies.NO_PRUNING, null, 25*1024*1024,
                    ALLOW_ALL );
            this.verifier = verifier;
        }

        @Override
        public synchronized void writeCommand( XaCommand command, int identifier ) throws IOException
        {
            this.verifier.visit( command );
        }
    }

    private static class CommandCapturingVisitor implements Visitor<XaCommand,RuntimeException>
    {
        private final Collection<XaCommand> commands = new ArrayList<>();

        @Override
        public boolean visit( XaCommand element ) throws RuntimeException
        {
            commands.add( element );
            return true;
        }

        public void injectInto( WriteTransaction tx )
        {
            for ( XaCommand command : commands )
            {
                tx.injectCommand( command );
            }
        }
    }

    private final IndexingService mockIndexing = mock( IndexingService.class );
    private final KernelTransactionImplementation kernelTransaction = mock( KernelTransactionImplementation.class );

    private WriteTransaction newWriteTransaction( IndexingService indexing )
    {
        return newWriteTransaction( indexing, nullVisitor );
    }

    private WriteTransaction newWriteTransaction( IndexingService indexing, Visitor<XaCommand,
            RuntimeException> verifier )
    {
        VerifyingXaLogicalLog log = new VerifyingXaLogicalLog( fs.get(), verifier );
        WriteTransaction result = new WriteTransaction( 0, 0l, log, transactionState, neoStore,
                cacheAccessBackDoor, indexing, NO_LABEL_SCAN_STORE, new IntegrityValidator(neoStore, indexing ),
                kernelTransaction );
        result.setCommitTxId( neoStore.getLastCommittedTx()+1 );
        return result;
    }

    private class CapturingIndexingService extends IndexingService
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<>();

        public CapturingIndexingService()
        {
            super(  null,
                    new DefaultSchemaIndexProviderMap( NO_INDEX_PROVIDER ),
                    new NeoStoreIndexStoreView( neoStore ),
                    null,
                    new KernelSchemaStateStore(),
                    new SingleLoggingService( DEV_NULL )
                );
        }

        @Override
        public void updateIndexes( Iterable<NodePropertyUpdate> updates )
        {
            this.updates.addAll( asCollection( updates ) );
        }
    }

    private static final long[] none = new long[0];

    private static final Visitor<XaCommand, RuntimeException> nullVisitor = new Visitor<XaCommand, RuntimeException>()
    {
        @Override
        public boolean visit( XaCommand element )
        {
            return true;
        }
    };

    private Visitor<XaCommand, RuntimeException> heavySchemaRuleVerifier()
    {
        return new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element )
            {
                for ( DynamicRecord record : ((SchemaRuleCommand) element).getRecordsAfter() )
                {
                    assertFalse( record + " should have been heavy", record.isLight() );
                }
                return true;
            }
        };
    }

    private void prepareAndCommitRecovered( WriteTransaction tx ) throws Exception
    {
        tx.setRecovered();
        prepareAndCommit( tx );
    }

    private void prepareAndCommit( WriteTransaction tx ) throws Exception
    {
        tx.doPrepare();
        tx.doCommit();
    }

    public static final LabelScanStore NO_LABEL_SCAN_STORE = new LabelScanStore()
    {
        @Override
        public void updateAndCommit( Iterator<NodeLabelUpdate> updates )
        {   // Do nothing
        }

        @Override
        public void stop()
        {   // Do nothing
        }

        @Override
        public void start()
        {   // Do nothing
        }

        @Override
        public void shutdown()
        {   // Do nothing
        }

        @Override
        public void recover( Iterator<NodeLabelUpdate> updates )
        {   // Do nothing
        }

        @Override
        public LabelScanReader newReader()
        {
            return LabelScanReader.EMPTY;
        }

        @Override
        public ResourceIterator<File> snapshotStoreFiles()
        {
            return emptyIterator();
        }

        @Override
        public void init()
        {   // Do nothing
        }

        @Override
        public void force()
        {   // Do nothing
        }
    };
}
