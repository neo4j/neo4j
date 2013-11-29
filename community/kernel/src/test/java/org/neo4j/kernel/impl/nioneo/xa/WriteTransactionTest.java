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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.api.direct.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.LabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelSchemaStateStore;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexUpdates;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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
    public void shouldMarkDynamicLabelRecordsAsNotInUseWhenLabelsAreReInlined() throws Exception
    {
        // GIVEN
        final long nodeId = neoStore.getNodeStore().nextId();

        // A transaction that creates labels that just barely fit to be inlined
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        writeTransaction.nodeCreate( nodeId );

        writeTransaction.addLabelToNode( 7, nodeId );
        writeTransaction.addLabelToNode( 11, nodeId );
        writeTransaction.addLabelToNode( 12, nodeId );
        writeTransaction.addLabelToNode( 15, nodeId );
        writeTransaction.addLabelToNode( 23, nodeId );
        writeTransaction.addLabelToNode( 27, nodeId );
        writeTransaction.addLabelToNode( 50, nodeId );

        writeTransaction.prepare();
        writeTransaction.commit();

        // And given that I now start recording the commands in the log
        CommandCapturingVisitor commandCapture = new CommandCapturingVisitor();

        // WHEN
        // I then remove multiple labels
        writeTransaction = newWriteTransaction( mockIndexing, commandCapture);

        writeTransaction.removeLabelFromNode( 11, nodeId );
        writeTransaction.removeLabelFromNode( 23, nodeId );

        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        // The dynamic label record should be part of what is logged, and it should be set to not in use anymore.
        commandCapture.visitCapturedCommands( new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element ) throws RuntimeException
            {
                if( element instanceof Command.NodeCommand )
                {
                    Command.NodeCommand cmd = (Command.NodeCommand)element;
                    Collection<DynamicRecord> beforeDynLabels = cmd.getAfter().getDynamicLabelRecords();
                    assertThat( beforeDynLabels.size(), equalTo(1) );
                    assertThat( beforeDynLabels.iterator().next().inUse(), equalTo(false) );
                }
                return true;
            }
        });
    }

    @Test
    public void shouldReUseOriginalDynamicRecordWhenInlinedAndThenExpandedLabelsInSameTx() throws Exception
    {
        // GIVEN
        final long nodeId = neoStore.getNodeStore().nextId();

        // A transaction that creates labels that just barely fit to be inlined
        WriteTransaction writeTransaction = newWriteTransaction( mockIndexing );
        writeTransaction.nodeCreate( nodeId );

        writeTransaction.addLabelToNode( 16, nodeId );
        writeTransaction.addLabelToNode( 29, nodeId );
        writeTransaction.addLabelToNode( 32, nodeId );
        writeTransaction.addLabelToNode( 41, nodeId );
        writeTransaction.addLabelToNode( 44, nodeId );
        writeTransaction.addLabelToNode( 45, nodeId );
        writeTransaction.addLabelToNode( 50, nodeId );
        writeTransaction.addLabelToNode( 51, nodeId );
        writeTransaction.addLabelToNode( 52, nodeId );

        writeTransaction.prepare();
        writeTransaction.commit();

        // And given that I now start recording the commands in the log
        CommandCapturingVisitor commandCapture = new CommandCapturingVisitor();

        // WHEN
        // I remove enough labels to inline them, but then add enough new labels to expand it back to dynamic
        writeTransaction = newWriteTransaction( mockIndexing, commandCapture);

        writeTransaction.removeLabelFromNode( 50, nodeId );
        writeTransaction.removeLabelFromNode( 51, nodeId );
        writeTransaction.removeLabelFromNode( 52, nodeId );
        writeTransaction.addLabelToNode( 60, nodeId );
        writeTransaction.addLabelToNode( 61, nodeId );
        writeTransaction.addLabelToNode( 62, nodeId );

        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        // The dynamic label record in before should be the same id as in after, and should be in use
        commandCapture.visitCapturedCommands( new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element ) throws RuntimeException
            {
                if( element instanceof Command.NodeCommand )
                {
                    Command.NodeCommand cmd = (Command.NodeCommand)element;
                    DynamicRecord before = cmd.getBefore().getDynamicLabelRecords().iterator().next();
                    DynamicRecord after = cmd.getAfter().getDynamicLabelRecords().iterator().next();

                    assertThat( before.getId(), equalTo(after.getId()) );
                    assertThat( after.inUse(), equalTo(true) );
                }
                return true;
            }
        });
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
        IteratorCollector<NodePropertyUpdate> indexUpdates = new IteratorCollector<>( 0 );
        doAnswer( indexUpdates ).when( index ).updateIndexes( any( IndexUpdates.class ) );
        CommandCapturingVisitor commandCapturingVisitor = new CommandCapturingVisitor();
        tx = newWriteTransaction( index, commandCapturingVisitor );
        tx.nodeCreate( nodeId );
        tx.addLabelToNode( labelId, nodeId );
        tx.nodeAddProperty( nodeId, propertyKeyId, "Neo" );
        prepareAndCommit( tx );
        verify( index, times( 1 ) ).updateIndexes( any( IndexUpdates.class ) );
        indexUpdates.assertContent( expectedUpdate );

        reset( index );
        indexUpdates = new IteratorCollector<>( 0 );
        doAnswer( indexUpdates ).when( index ).updateIndexes( any( IndexUpdates.class ) );

        // WHEN
        // -- later recovering that tx, there should be only one update
        tx = newWriteTransaction( index );
        commandCapturingVisitor.injectInto( tx );
        prepareAndCommitRecovered( tx );
        verify( index, times( 1 ) ).updateIndexes( any( IndexUpdates.class ) );
        indexUpdates.assertContent( expectedUpdate );
    }

    @Test
    public void shouldLockUpdatedNodes() throws Exception
    {
        // given
        NodeStore nodeStore = neoStore.getNodeStore();
        long[] nodes = { // allocate ids
                nodeStore.nextId(),
                nodeStore.nextId(),
                nodeStore.nextId(),
                nodeStore.nextId(),
                nodeStore.nextId(),
                nodeStore.nextId(),
                nodeStore.nextId(),
        };
        // create the node records that we will modify in our main tx.
        {
            WriteTransaction tx = newWriteTransaction( mockIndexing );
            for ( int i = 1; i < nodes.length - 1; i++ )
            {
                tx.nodeCreate( nodes[i] );
            }
            tx.nodeAddProperty( nodes[3], 0, "old" );
            tx.nodeAddProperty( nodes[4], 0, "old" );
            prepareAndCommit( tx );
            reset( locks ); // reset the lock counts
        }

        // These are the changes we want to assert locking on
        WriteTransaction tx = newWriteTransaction( mockIndexing );
        tx.nodeCreate( nodes[0] );
        tx.addLabelToNode( 0, nodes[1] );
        tx.nodeAddProperty( nodes[2], 0, "value" );
        tx.nodeChangeProperty( nodes[3], 0, "value" );
        tx.nodeRemoveProperty( nodes[4], 0 );
        tx.nodeDelete( nodes[5] );

        tx.nodeCreate( nodes[6] );
        tx.addLabelToNode( 0, nodes[6] );
        tx.nodeAddProperty( nodes[6], 0, "value" );

        // when
        prepareAndCommit( tx );

        // then
        // create node, NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[0], LockService.LockType.WRITE_LOCK );
        // add label, NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[1], LockService.LockType.WRITE_LOCK );
        // add property, NodeCommand and PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[2], LockService.LockType.WRITE_LOCK );
        // update property, in place, PropertyCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[3], LockService.LockType.WRITE_LOCK );
        // remove property, updates the Node and the Property == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[4], LockService.LockType.WRITE_LOCK );
        // delete node, single NodeCommand == 1 update
        verify( locks, times( 1 ) ).acquireNodeLock( nodes[5], LockService.LockType.WRITE_LOCK );
        // create and add-label goes into the NodeCommand, add property is a PropertyCommand == 2 updates
        verify( locks, times( 2 ) ).acquireNodeLock( nodes[6], LockService.LockType.WRITE_LOCK );
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
    @SuppressWarnings("deprecation")
    private final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private final DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
    private NeoStore neoStore;
    private LockService locks;
    private CacheAccessBackDoor cacheAccessBackDoor;
    private final List<Lock> lockMocks = new ArrayList<>();

    @Before
    public void before() throws Exception
    {
        transactionState = TransactionState.NO_STATE;
        @SuppressWarnings("deprecation")
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory,
                fs.get(), DEV_NULL, new DefaultTxHook() );
        neoStore = storeFactory.createNeoStore( new File( "neostore" ) );
        locks = mock( LockService.class, new Answer()
        {
            @Override
            public synchronized Object answer( InvocationOnMock invocation ) throws Throwable
            {
                Lock mock = mock( Lock.class );
                lockMocks.add( mock );
                return mock;
            }
        } );
        cacheAccessBackDoor = mock( CacheAccessBackDoor.class );
    }

    @After
    public void shouldReleaseAllLocks()
    {
        for ( Lock lock : lockMocks )
        {
            verify( lock ).release();
        }
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

        public void visitCapturedCommands( Visitor<XaCommand, RuntimeException> visitor )
        {
            for ( XaCommand command : commands )
            {
                visitor.visit( command );
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
                kernelTransaction, locks );
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
                    new NeoStoreIndexStoreView( locks, neoStore ),
                    null,
                    new KernelSchemaStateStore(),
                    new SingleLoggingService( DEV_NULL )
                );
        }

        @Override
        public void updateIndexes( IndexUpdates updates )
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
        public AllEntriesLabelScanReader newAllEntriesReader()
        {
            return null;
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

    private class IteratorCollector<T> implements Answer<Object>
    {
        private final int arg;
        private final List<T> elements = new ArrayList<>();

        public IteratorCollector( int arg )
        {
            this.arg = arg;
        }

        @SafeVarargs
        public final void assertContent( T... expected )
        {
            assertEquals( Arrays.asList( expected ), elements );
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object answer( InvocationOnMock invocation ) throws Throwable
        {
            Object iterator = invocation.getArguments()[arg];
            if ( iterator instanceof Iterable )
            {
                iterator = ((Iterable) iterator).iterator();
            }
            if ( iterator instanceof Iterator )
            {
                collect( (Iterator) iterator );
            }
            return null;
        }

        private void collect( Iterator<T> iterator )
        {
            while ( iterator.hasNext() )
            {
                elements.add( iterator.next() );
            }
        }
    }
}
