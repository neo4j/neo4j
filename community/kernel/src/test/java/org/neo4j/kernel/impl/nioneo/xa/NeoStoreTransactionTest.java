/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Functions;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.IdType;
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
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.Lock;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.store.labels.InlineNodeLabels;
import org.neo4j.kernel.impl.transaction.KernelHealth;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static java.lang.Integer.parseInt;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.IdType.NODE;
import static org.neo4j.kernel.IdType.RELATIONSHIP;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.add;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.change;
import static org.neo4j.kernel.api.index.NodePropertyUpdate.remove;
import static org.neo4j.kernel.api.index.SchemaIndexProvider.NO_INDEX_PROVIDER;
import static org.neo4j.kernel.impl.api.index.TestSchemaIndexProviderDescriptor.PROVIDER_DESCRIPTOR;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.indexRule;
import static org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule.uniquenessConstraintRule;
import static org.neo4j.kernel.impl.transaction.xaframework.InjectedTransactionValidator.ALLOW_ALL;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class NeoStoreTransactionTest
{
    public static final String LONG_STRING = "string value long enough not to be stored as a short string";

    @Test
    public void shouldValidateConstraintIndexAsPartOfPrepare() throws Exception
    {
        // GIVEN
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();

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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();

        // WHEN
        final long ruleId = neoStore.getSchemaStore().nextId();
        IndexRule schemaRule = indexRule( ruleId, 10, 8, PROVIDER_DESCRIPTOR );
        writeTransaction.createSchemaRule( schemaRule );
        prepareAndCommit( writeTransaction );

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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();

        // WHEN
        writeTransaction.dropSchemaRule( rule );
        prepareAndCommit( writeTransaction );

        // THEN
        verify( cacheAccessBackDoor ).removeSchemaRuleFromCache( ruleId );
    }

    @Test
    public void shouldMarkDynamicLabelRecordsAsNotInUseWhenLabelsAreReInlined() throws Exception
    {
        // GIVEN
        final long nodeId = neoStore.getNodeStore().nextId();

        // A transaction that creates labels that just barely fit to be inlined
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        writeTransaction.nodeCreate( nodeId );

        writeTransaction.addLabelToNode( 7, nodeId );
        writeTransaction.addLabelToNode( 11, nodeId );
        writeTransaction.addLabelToNode( 12, nodeId );
        writeTransaction.addLabelToNode( 15, nodeId );
        writeTransaction.addLabelToNode( 23, nodeId );
        writeTransaction.addLabelToNode( 27, nodeId );
        writeTransaction.addLabelToNode( 50, nodeId );

        prepareAndCommit( writeTransaction );

        // And given that I now start recording the commands in the log
        CommandCapturingVisitor commandCapture = new CommandCapturingVisitor();

        // WHEN
        // I then remove multiple labels
        writeTransaction = newWriteTransaction( mockIndexing, commandCapture ).first();

        writeTransaction.removeLabelFromNode( 11, nodeId );
        writeTransaction.removeLabelFromNode( 23, nodeId );

        prepareAndCommit( writeTransaction );

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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
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

        prepareAndCommit( writeTransaction );

        // And given that I now start recording the commands in the log
        CommandCapturingVisitor commandCapture = new CommandCapturingVisitor();

        // WHEN
        // I remove enough labels to inline them, but then add enough new labels to expand it back to dynamic
        writeTransaction = newWriteTransaction( mockIndexing, commandCapture).first();

        writeTransaction.removeLabelFromNode( 50, nodeId );
        writeTransaction.removeLabelFromNode( 51, nodeId );
        writeTransaction.removeLabelFromNode( 52, nodeId );
        writeTransaction.addLabelToNode( 60, nodeId );
        writeTransaction.addLabelToNode( 61, nodeId );
        writeTransaction.addLabelToNode( 62, nodeId );

        prepareAndCommit( writeTransaction );

        // THEN
        // The dynamic label record in before should be the same id as in after, and should be in use
        commandCapture.visitCapturedCommands( new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element ) throws RuntimeException
            {
                if( element instanceof Command.NodeCommand )
                {
                    Command.NodeCommand cmd = (Command.NodeCommand) element;
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();

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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing, verifier ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        DefinedProperty property1 = writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        Object newValue1 = "new", newValue2 = "new 2";
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        int propertyKey1 = 1, propertyKey2 = 2;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        DefinedProperty property1 = writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        DefinedProperty property2 = writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        int propertyKey1 = 1, propertyKey2 = 2, labelId = 3;
        long[] labelIds = new long[] {labelId};
        Object value1 = LONG_STRING, value2 = LONG_STRING.getBytes();
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.nodeAddProperty( nodeId, propertyKey2, value2 );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
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
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
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
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction writeTransaction = newWriteTransaction( mockIndexing ).first();
        int propertyKey1 = 1, propertyKey2 = 2, labelId1 = 3, labelId2 = 4;
        Object value1 = "first", value2 = 4;
        writeTransaction.nodeCreate( nodeId );
        writeTransaction.nodeAddProperty( nodeId, propertyKey1, value1 );
        writeTransaction.addLabelToNode( labelId1, nodeId );
        writeTransaction.addLabelToNode( labelId2, nodeId );
        prepareAndCommit( writeTransaction );

        // WHEN
        CapturingIndexingService indexingService = new CapturingIndexingService();
        writeTransaction = newWriteTransaction( indexingService ).first();
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
        NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();
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
        assertEquals( "PropertyIndexStore", propertyKeyId+1, neoStore.getPropertyKeyTokenStore().getHighId() );
        assertEquals( "PropertyKeyToken NameStore", 2, neoStore.getPropertyStore().getPropertyKeyTokenStore().getNameStore().getHighId() );
        assertEquals( "SchemaStore", ruleId+1, neoStore.getSchemaStore().getHighId() );
    }

    @Test
    public void createdSchemaRuleRecordMustBeWrittenHeavy() throws Exception
    {
        // THEN
        Visitor<XaCommand, RuntimeException> verifier = heavySchemaRuleVerifier();

        // GIVEN
        NeoStoreTransaction tx = newWriteTransaction( mockIndexing, verifier ).first();
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
        NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();
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
        tx = newWriteTransaction( mockIndexing, verifier ).first();
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
        CapturingIndexingService indexingService = new CapturingIndexingService();
        NeoStoreTransaction tx = newWriteTransaction( indexingService ).first();
        SchemaRule rule = indexRule( ruleId, labelId, propertyKeyId, PROVIDER_DESCRIPTOR );
        tx.createSchemaRule( rule );
        prepareAndCommit( tx );

        // -- and a tx creating a node with that label and property key
        IndexingService index = mock( IndexingService.class, RETURNS_MOCKS );
        IteratorCollector<NodePropertyUpdate> indexUpdates = new IteratorCollector<>( 0 );
        doAnswer( indexUpdates ).when( index ).validate( any( IndexUpdates.class ) );
        CommandCapturingVisitor commandCapturingVisitor = new CommandCapturingVisitor();
        tx = newWriteTransaction( index, commandCapturingVisitor ).first();
        tx.nodeCreate( nodeId );
        tx.addLabelToNode( labelId, nodeId );
        tx.nodeAddProperty( nodeId, propertyKeyId, "Neo" );
        prepareAndCommit( tx );
        verify( index, times( 1 ) ).validate( any( IndexUpdates.class ) );
        indexUpdates.assertContent( expectedUpdate );

        reset( index );
        indexUpdates = new IteratorCollector<>( 0 );
        doAnswer( indexUpdates ).when( index ).validate( any( IndexUpdates.class ) );

        // WHEN
        // -- later recovering that tx, there should be only one update
        tx = newWriteTransaction( index ).first();
        commandCapturingVisitor.injectInto( tx );
        prepareAndCommitRecovered( tx );
        verify( index, times( 1 ) ).validate( any( IndexUpdates.class ) );
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
            NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();
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
        NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();
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

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithDifferentTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        instantiateNeoStore( 50 );
        Pair<NeoStoreTransaction, NeoStoreTransactionContext> transactionContextPair =
                newWriteTransaction();
        NeoStoreTransaction tx = transactionContextPair.first();
        NeoStoreTransactionContext txCtx = transactionContextPair.other();
        int nodeId = (int) nextId( NODE ), typeA = 0, typeB = 1, typeC = 2;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( typeA, "A" );
        createRelationships( tx, nodeId, typeA, OUTGOING, 6 );
        createRelationships( tx, nodeId, typeA, INCOMING, 7 );

        tx.createRelationshipTypeToken( typeB, "B" );
        createRelationships( tx, nodeId, typeB, OUTGOING, 8 );
        createRelationships( tx, nodeId, typeB, INCOMING, 9 );

        tx.createRelationshipTypeToken( typeC, "C" );
        createRelationships( tx, nodeId, typeC, OUTGOING, 10 );
        createRelationships( tx, nodeId, typeC, INCOMING, 10 );
        // here we're at the edge
        assertFalse( tx.nodeLoadLight( nodeId ).isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( tx, nodeId, typeC, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( tx.nodeLoadLight( nodeId ).isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 6, 7 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 8, 9 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 10, 11 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeDifferentDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        instantiateNeoStore( 49 );
        Pair<NeoStoreTransaction, NeoStoreTransactionContext> transactionContextPair =
                newWriteTransaction();
        NeoStoreTransaction tx = transactionContextPair.first();
        NeoStoreTransactionContext txCtx = transactionContextPair.other();
        int nodeId = (int) nextId( NODE ), typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( typeA, "A" );
        createRelationships( tx, nodeId, typeA, OUTGOING, 24 );
        createRelationships( tx, nodeId, typeA, INCOMING, 25 );

        // here we're at the edge
        assertFalse( tx.nodeLoadLight( nodeId ).isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( tx, nodeId, typeA, INCOMING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( tx.nodeLoadLight( nodeId ).isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 24, 26 );
    }

    @Test
    public void shouldConvertToDenseNodeRepresentationWhenHittingThresholdWithTheSameTypeSameDirection()
            throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        instantiateNeoStore( 8 );
        Pair<NeoStoreTransaction, NeoStoreTransactionContext> transactionContextPair =
                newWriteTransaction();
        NeoStoreTransaction tx = transactionContextPair.first();
        NeoStoreTransactionContext txCtx = transactionContextPair.other();
        int nodeId = (int) nextId( NODE ), typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( typeA, "A" );
        createRelationships( tx, nodeId, typeA, OUTGOING, 8 );

        // here we're at the edge
        assertFalse( tx.nodeLoadLight( nodeId ).isDense() );

        // WHEN creating the relationship that pushes us over the threshold
        createRelationships( tx, nodeId, typeA, OUTGOING, 1 );

        // THEN the node should have been converted into a dense node
        assertTrue( tx.nodeLoadLight( nodeId ).isDense() );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 9, 0 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithOneType() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        instantiateNeoStore( 13 );
        Pair<NeoStoreTransaction, NeoStoreTransactionContext> transactionContextPair =
                newWriteTransaction();
        NeoStoreTransaction tx = transactionContextPair.first();
        NeoStoreTransactionContext txCtx = transactionContextPair.other();
        int nodeId = (int) nextId( NODE ), typeA = 0;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( typeA, "A" );
        long[] relationshipsCreated = createRelationships( tx, nodeId, typeA, INCOMING, 15 );

        //WHEN
        deleteRelationship( tx, relationshipsCreated[0] );

        // THEN the node should have been converted into a dense node
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 0, 14 );
    }

    @Test
    public void shouldMaintainCorrectDataWhenDeletingFromDenseNodeWithManyTypes() throws Exception
    {
        // GIVEN a node with a total of denseNodeThreshold-1 relationships
        instantiateNeoStore( 1 );
        Pair<NeoStoreTransaction, NeoStoreTransactionContext> transactionAndContextPair =
                newWriteTransaction();
        NeoStoreTransaction tx = transactionAndContextPair.first();
        NeoStoreTransactionContext txCtx = transactionAndContextPair.other();
        int nodeId = (int) nextId( NODE ), typeA = 0, typeB = 12, typeC = 600;
        tx.nodeCreate( nodeId );
        tx.createRelationshipTypeToken( typeA, "A" );
        long[] relationshipsCreatedAIncoming = createRelationships( tx, nodeId, typeA, INCOMING, 1 );
        long[] relationshipsCreatedAOutgoing = createRelationships( tx, nodeId, typeA, OUTGOING, 1 );

        tx.createRelationshipTypeToken( typeB, "B" );
        long[] relationshipsCreatedBIncoming = createRelationships( tx, nodeId, typeB, INCOMING, 1 );
        long[] relationshipsCreatedBOutgoing = createRelationships( tx, nodeId, typeB, OUTGOING, 1 );

        tx.createRelationshipTypeToken( typeC, "C" );
        long[] relationshipsCreatedCIncoming = createRelationships( tx, nodeId, typeC, INCOMING, 1 );
        long[] relationshipsCreatedCOutgoing = createRelationships( tx, nodeId, typeC, OUTGOING, 1 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedAIncoming[0] );

        // THEN
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeA, 1, 0 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedAOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeA );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 1 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedBIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeA );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeB, 1, 0 );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedBOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeA );
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeB );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 1 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedCIncoming[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeA );
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeB );
        assertDenseRelationshipCounts( tx, txCtx, nodeId, typeC, 1, 0 );

        // WHEN
        deleteRelationship( tx, relationshipsCreatedCOutgoing[0] );

        // THEN
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeA );
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeB );
        assertRelationshipGroupDoesNotExist( txCtx, tx.nodeLoadLight( nodeId ), typeC );
    }

    @Test
    public void movingBilaterallyOfTheDenseNodeThresholdIsConsistent() throws Exception
    {
        // GIVEN
        instantiateNeoStore( 10 );
        final long nodeId = neoStore.getNodeStore().nextId();

        NeoStoreTransaction writeTransaction = newWriteTransaction().first();
        writeTransaction.nodeCreate( nodeId );

        int typeA = 0;
        writeTransaction.createRelationshipTypeToken( typeA, "A" );
        createRelationships( writeTransaction, nodeId, typeA, INCOMING, 20 );

        prepareAndCommit( writeTransaction );

        int typeB = 1;
        writeTransaction.createRelationshipTypeToken( typeB, "B" );

        // And given that I now start recording the commands in the log
        CommandCapturingVisitor commandCapture = new CommandCapturingVisitor();
        writeTransaction = newWriteTransaction( mockIndexing, commandCapture ).first();

        // WHEN
        // i remove enough relationships to become dense and remove enough to become not dense
        long[] relationshipsOfTypeB = createRelationships( writeTransaction, nodeId, typeB, OUTGOING, 5 );
        for ( long relationshipToDelete : relationshipsOfTypeB )
        {
            deleteRelationship( writeTransaction, relationshipToDelete );
        }

        prepareAndCommit( writeTransaction );

        // THEN
        // The dynamic label record in before should be the same id as in after, and should be in use
        final AtomicBoolean foundRelationshipGroupInUse = new AtomicBoolean();
        commandCapture.visitCapturedCommands( new Visitor<XaCommand, RuntimeException>()
        {
            @Override
            public boolean visit( XaCommand element ) throws RuntimeException
            {
                if( element instanceof Command.RelationshipGroupCommand &&
                        ( (Command.RelationshipGroupCommand) element).getRecord().inUse() )
                {
                    if ( !foundRelationshipGroupInUse.get() )
                    {
                        foundRelationshipGroupInUse.set( true );
                    }
                    else
                    {
                        fail();
                    }
                }
                return true;
            }
        });
        assertTrue( "Did not create relationship group command", foundRelationshipGroupInUse.get() );
    }

    @Test
    public void shouldSortRelationshipGroups() throws Exception
    {
        // GIVEN a node with group of type 10
        instantiateNeoStore( 1 );
        int type5 = 5, type10 = 10, type15 = 15;
        {
            NeoStoreTransaction tx = newWriteTransaction().first();
            neoStore.getRelationshipTypeStore().setHighId( 16 );
            tx.createRelationshipTypeToken( type5, "5" );
            tx.createRelationshipTypeToken( type10, "10" );
            tx.createRelationshipTypeToken( type15, "15" );
            prepareAndCommit( tx );
        }
        long nodeId = neoStore.getNodeStore().nextId();
        {
            NeoStoreTransaction tx = newWriteTransaction().first();
            long otherNode1Id = neoStore.getNodeStore().nextId();
            long otherNode2Id = neoStore.getNodeStore().nextId();
            tx.nodeCreate( nodeId );
            tx.nodeCreate( otherNode1Id );
            tx.nodeCreate( otherNode2Id );
            tx.relationshipCreate( neoStore.getRelationshipStore().nextId(), type10, nodeId, otherNode1Id );
            // This relationship will cause the switch to dense
            tx.relationshipCreate( neoStore.getRelationshipStore().nextId(), type10, nodeId, otherNode2Id );
            prepareAndCommit( tx );
            // Just a little validation of assumptions
            assertRelationshipGroupsInOrder( nodeId, type10 );
        }

        // WHEN inserting a relationship of type 5
        {
            NeoStoreTransaction tx = newWriteTransaction().first();
            long otherNodeId = neoStore.getNodeStore().nextId();
            tx.nodeCreate( otherNodeId );
            tx.relationshipCreate( neoStore.getRelationshipStore().nextId(), type5, nodeId, otherNodeId );
            prepareAndCommit( tx );
        }

        // THEN that group should end up first in the chain
        assertRelationshipGroupsInOrder( nodeId, type5, type10 );

        // WHEN inserting a relationship of type 15
        {
            NeoStoreTransaction tx = newWriteTransaction().first();
            long otherNodeId = neoStore.getNodeStore().nextId();
            tx.nodeCreate( otherNodeId );
            tx.relationshipCreate( neoStore.getRelationshipStore().nextId(), type15, nodeId, otherNodeId );
            prepareAndCommit( tx );
        }

        // THEN that group should end up last in the chain
        assertRelationshipGroupsInOrder( nodeId, type5, type10, type15 );
    }

    private void assertRelationshipGroupsInOrder( long nodeId, int... types )
    {
        NodeRecord node = neoStore.getNodeStore().getRecord( nodeId );
        assertTrue( "Node should be dense, is " + node, node.isDense() );
        long groupId = node.getNextRel();
        int cursor = 0;
        List<RelationshipGroupRecord> seen = new ArrayList<>();
        while ( groupId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            RelationshipGroupRecord group = neoStore.getRelationshipGroupStore().getRecord( groupId );
            seen.add( group );
            assertEquals( "Invalid type, seen groups so far " + seen, types[cursor++], group.getType() );
            groupId = group.getNext();
        }
        assertEquals( "Not enough relationship group records found in chain for " + node, types.length, cursor );
    }

    private static void assertRelationshipGroupDoesNotExist( NeoStoreTransactionContext txCtx, NodeRecord node, int type )
    {
        assertNull( txCtx.getRelationshipGroup( node, type ) );
    }

    private static void assertDenseRelationshipCounts( NeoStoreTransaction tx, NeoStoreTransactionContext txCtx,
                                                       long nodeId, int type, int outCount, int inCount )
    {
        RelationshipGroupRecord group = txCtx.getRelationshipGroup( tx.nodeLoadLight( nodeId ), type ).forReadingData();
        assertNotNull( group );

        RelationshipRecord rel;
        long relId = group.getFirstOut();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = tx.relLoadLight( relId );
            // count is stored in the back pointer of the first relationship in the chain
            assertEquals( "Stored relationship count for OUTGOING differs", outCount, rel.getFirstPrevRel() );
            assertEquals( "Manually counted relationships for OUTGOING differs", outCount,
                    manuallyCountRelationships( tx, nodeId, relId ) );
        }

        relId = group.getFirstIn();
        if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            rel = tx.relLoadLight( relId );
            assertEquals( "Stored relationship count for INCOMING differs", inCount, rel.getSecondPrevRel() );
            assertEquals( "Manually counted relationships for INCOMING differs", inCount,
                    manuallyCountRelationships( tx, nodeId, relId ) );
        }
    }

    private static int manuallyCountRelationships( NeoStoreTransaction tx, long nodeId, long firstRelId )
    {
        int count = 0;
        long relId = firstRelId;
        while ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
        {
            count++;
            RelationshipRecord record = tx.relLoadLight( relId );
            relId = record.getFirstNode() == nodeId ? record.getFirstNextRel() : record.getSecondNextRel();
        }
        return count;
    }

    private long nextId( IdType type )
    {
        return idGeneratorFactory.get( type ).nextId();
    }

    private long[] createRelationships( NeoStoreTransaction tx, long nodeId, int type, Direction direction, int count )
    {
        long[] result = new long[ count ];
        for ( int i = 0; i < count; i++ )
        {
            long otherNodeId = nextId( NODE );
            tx.nodeCreate( otherNodeId );
            long first = direction == OUTGOING ? nodeId : otherNodeId;
            long other = direction == INCOMING ? nodeId : otherNodeId;
            long relId = nextId( RELATIONSHIP );
            result[i] = relId;
            tx.relationshipCreate( relId, type, first, other );
        }
        return result;
    }

    private void deleteRelationship( NeoStoreTransaction tx, long relId )
    {
        tx.relDelete( relId );
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

    @Test
    public void shouldValidateIndexUpdatesAsPartOfPrepare() throws Exception
    {
        // Given
        NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();

        tx.nodeCreate( 1 );
        tx.addLabelToNode( 1, 1 );
        tx.nodeAddProperty( 1, 1, "foo" );

        tx.nodeCreate( 2 );
        tx.addLabelToNode( 2, 2 );
        tx.nodeAddProperty( 2, 2, "bar" );

        // When
        tx.prepare();

        // Then
        ArgumentCaptor<IndexUpdates> captor = ArgumentCaptor.forClass( IndexUpdates.class );
        verify( mockIndexing ).validate( captor.capture() );
        IndexUpdates updates = captor.getValue();

        assertEquals(
                asSet( add( 1, 1, "foo", new long[]{1} ), add( 2, 2, "bar", new long[]{2} ) ),
                asSet( updates )
        );
        assertEquals( asSet( 1L, 2L ), updates.changedNodeIds() );
    }

    @Test
    public void shouldValidateAndApplyIndexUpdatesAsPartOfCommitForRecoveredTx() throws Exception
    {
        // Given
        long nodeId = neoStore.getNodeStore().nextId();
        long labelId = neoStore.getLabelTokenStore().nextId();
        long propertyId = neoStore.getPropertyStore().nextId();
        long propertyKeyId = neoStore.getPropertyStore().getPropertyKeyTokenStore().nextId();
        String value = "foo";

        ValidatedIndexUpdates validatedIndexUpdates = mock( ValidatedIndexUpdates.class );
        when( mockIndexing.validate( any( IndexUpdates.class ) ) ).thenReturn( validatedIndexUpdates );

        NeoStoreTransaction tx = newWriteTransaction( mockIndexing ).first();
        tx.setRecovered();

        tx.injectCommand( nodeCreateCommand( nodeId, labelId ) );

        tx.injectCommand( propertyCommand( propertyId, nodeId, (int) propertyKeyId, value ) );

        // When
        tx.commit();

        // Then
        ArgumentCaptor<IndexUpdates> captor = ArgumentCaptor.forClass( IndexUpdates.class );
        verify( mockIndexing ).validate( captor.capture() );
        IndexUpdates updates = captor.getValue();

        assertEquals( asSet( add( nodeId, (int) propertyKeyId, value, new long[]{labelId} ) ), asSet( updates ) );
        assertEquals( asSet( nodeId ), updates.changedNodeIds() );

        verify( mockIndexing ).updateIndexes( validatedIndexUpdates );
    }

    private Command.NodeCommand nodeCreateCommand( long nodeId, long labelId )
    {
        NodeRecord before = new NodeRecord( nodeId, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                Record.NO_NEXT_PROPERTY.intValue() );
        before.setInUse( false );

        NodeRecord after = new NodeRecord( nodeId, false, Record.NO_NEXT_RELATIONSHIP.intValue(),
                Record.NO_NEXT_PROPERTY.intValue() );
        after.setInUse( true );

        InlineNodeLabels nodeLabels = new InlineNodeLabels( -1, after );
        nodeLabels.put( new long[]{labelId}, neoStore.getNodeStore(), null );

        return new Command.NodeCommand().init( before, after );
    }

    private Command.PropertyCommand propertyCommand( long recordId, long nodeId, int propertyKeyId, Object value )
    {
        PropertyRecord before = new PropertyRecord( recordId );
        before.setInUse( false );

        PropertyBlock block = new PropertyBlock();
        neoStore.getPropertyStore().encodeValue( block, propertyKeyId, value );
        PropertyRecord after = new PropertyRecord( recordId );
        after.setInUse( true );
        after.addPropertyBlock( block );
        after.setNodeId( nodeId );

        return new Command.PropertyCommand().init( before, after );
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TransactionState transactionState = mock( TransactionState.class );
    private Config config;
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
        instantiateNeoStore( parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() ) );
    }

    private void instantiateNeoStore( int denseNodeThreshold )
    {
        if ( neoStore != null )
        {
            fs.clear();
        }

        config = new Config( stringMap(
                GraphDatabaseSettings.dense_node_threshold.name(), "" + denseNodeThreshold ) );
        @SuppressWarnings("deprecation")
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory,
                fs.get(), DEV_NULL, new DefaultTxHook() );
        neoStore = storeFactory.createNeoStore( new File( "neostore" ) );
        lockMocks.clear();
        locks = mock( LockService.class, new Answer()
        {
            @Override
            public synchronized Object answer( final InvocationOnMock invocation ) throws Throwable
            {
                // This is necessary because finalize() will also be called
                if ( invocation.getMethod().getName().equals( "acquireNodeLock" ) )
                {
                    final Lock mock = mock( Lock.class, new Answer()
                    {
                        @Override
                        public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
                        {
                            return null;
                        }
                    } );
                    lockMocks.add( mock );
                    return mock;
                }
                else
                {
                    return null;
                }
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
            super( new File( "log" ), null, mock( XaCommandReaderFactory.class ), mock( XaCommandWriterFactory.class ),
                    null, fs, new Monitors(), new SingleLoggingService( DEV_NULL ),
                    LogPruneStrategies.NO_PRUNING, null, mock( KernelHealth.class ), 25*1024*1024, ALLOW_ALL,
                    Functions.<List<LogEntry>>identity(), Functions.<List<LogEntry>>identity() );
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

        public void injectInto( NeoStoreTransaction tx )
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

    private final IndexingService mockIndexing = mock( IndexingService.class, RETURNS_MOCKS );
    private final KernelTransactionImplementation kernelTransaction = mock( KernelTransactionImplementation.class );

    private Pair<NeoStoreTransaction, NeoStoreTransactionContext> newWriteTransaction()
    {
        return newWriteTransaction( mockIndexing );
    }

    private Pair<NeoStoreTransaction, NeoStoreTransactionContext> newWriteTransaction( IndexingService indexing )
    {
        return newWriteTransaction( indexing, nullVisitor );
    }

    private Pair<NeoStoreTransaction, NeoStoreTransactionContext> newWriteTransaction( IndexingService indexing, Visitor<XaCommand,
            RuntimeException> verifier )
    {
        VerifyingXaLogicalLog log = new VerifyingXaLogicalLog( fs.get(), verifier );
        NeoStoreTransactionContext context =
                new NeoStoreTransactionContext( mock( NeoStoreTransactionContextSupplier.class ), neoStore );
        when(transactionState.locks()).thenReturn( mock(Locks.Client.class) );
        context.bind( transactionState );
        NeoStoreTransaction result = new NeoStoreTransaction( 0l, log, neoStore,
                cacheAccessBackDoor, indexing, NO_LABEL_SCAN_STORE, new IntegrityValidator( neoStore, indexing ),
                kernelTransaction, locks, context );
        result.setIdentifier( 0 );
        result.setCommitTxId( neoStore.getLastCommittedTx()+1 );
        return Pair.of( result, context );
    }

    private class CapturingIndexingService extends IndexingService
    {
        private final Set<NodePropertyUpdate> updates = new HashSet<>();

        public CapturingIndexingService()
        {
            super( null,
                    new DefaultSchemaIndexProviderMap( NO_INDEX_PROVIDER ),
                    new NeoStoreIndexStoreView( locks, neoStore ),
                    null,
                    new KernelSchemaStateStore(),
                    new SingleLoggingService( DEV_NULL ), IndexingService.NO_MONITOR
            );
        }

        @Override
        public ValidatedIndexUpdates validate( IndexUpdates updates )
        {
            this.updates.addAll( asCollection( updates ) );
            return super.validate( updates );
        }

        @Override
        public void updateIndexes( ValidatedIndexUpdates updates )
        {
            super.updateIndexes( updates );
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

    private void prepareAndCommitRecovered( NeoStoreTransaction tx ) throws Exception
    {
        tx.setRecovered();
        prepareAndCommit( tx );
    }

    private void prepareAndCommit( NeoStoreTransaction tx ) throws Exception
    {
        tx.doPrepare();
        tx.doCommit();
    }

    public static final LabelScanStore NO_LABEL_SCAN_STORE = new LabelScanStore()
    {
        @Override
        public LabelScanReader newReader()
        {
            return LabelScanReader.EMPTY;
        }

        @Override
        public LabelScanWriter newWriter()
        {
            return LabelScanWriter.EMPTY;
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
            assertEquals( asList( expected ), elements );
        }

        @Override
        @SuppressWarnings("unchecked")
        public ValidatedIndexUpdates answer( InvocationOnMock invocation ) throws Throwable
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
            return mock( ValidatedIndexUpdates.class );
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
