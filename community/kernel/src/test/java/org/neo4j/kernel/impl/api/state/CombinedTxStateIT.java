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
package org.neo4j.kernel.impl.api.state;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TransactionStateController;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.storageengine.api.txstate.PrimitiveLongReadableDiffSets;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.values.storable.ValueTuple;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;

public class CombinedTxStateIT
{
    @Rule
    public DatabaseRule databaseRule = new EmbeddedDatabaseRule();

    @Before
    public void setUp() throws Exception
    {
        TransactionStatesContainer.MULTI_STATE = true;
    }

    @After
    public void tearDown() throws Exception
    {
        TransactionStatesContainer.MULTI_STATE = false;
    }

    @Test
    public void combinedStateHasChangesWhenStableIsModified()
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            databaseRule.createNode();
            TransactionStateController stateController = txStateController();
            stateController.split();
            TransactionState transactionState = getTransactionState();

            assertCombinedTxState( transactionState );
            assertTrue( transactionState.hasDataChanges() );
            assertTrue( transactionState.hasChanges() );

            stateController.combine();
        }
    }

    @Test
    public void combinedStateHasChangesWhenStableIsNotModified()
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            stateController.split();
            databaseRule.createNode();
            TransactionState transactionState = getTransactionState();

            assertCombinedTxState( transactionState );
            assertTrue( transactionState.hasDataChanges() );
            assertTrue( transactionState.hasChanges() );

            stateController.combine();
        }
    }

    @Test( expected = UnsupportedOperationException.class )
    public void combinedStateVisitingIsNotSupported()
            throws ConstraintValidationException, CreateConstraintFailureException
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            stateController.split();
            databaseRule.createNode();
            TransactionState transactionState = getTransactionState();
            transactionState.accept( new TxStateVisitor.Adapter() );
        }
    }

    @Test( expected = UnsupportedOperationException.class )
    public void combinedStateRelationshipVisitingIsNotSupported()
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            stateController.split();
            databaseRule.createNode();
            TransactionState transactionState = getTransactionState();
            transactionState.relationshipVisit( 1, new RelationshipDataExtractor() );
        }
    }

    @Test
    public void augmentAllRelationshipsFromStableAndNewStateOnRead()
    {
        RelationshipType relationshipType = RelationshipType.withName( "any" );
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node1Before = databaseRule.createNode();
            Node node2Before = databaseRule.createNode();
            long relationshipIdBeforeSplit = node1Before.createRelationshipTo( node2Before, relationshipType ).getId();

            stateController.split();

            Node node1After = databaseRule.createNode();
            Node node2After = databaseRule.createNode();
            long relationshipIdAfterSplit = node1After.createRelationshipTo( node2After, relationshipType ).getId();

            TransactionState transactionState = getTransactionState();
            long augmentedRelationshipId = 12;
            RelationshipIterator combinedRelationshipIterator =
                    transactionState.augmentRelationshipsGetAll( new ArrayRelationshipVisitor( new long[]{augmentedRelationshipId} ) );
            long[] ids = PrimitiveLongCollections.asArray( combinedRelationshipIterator );

            assertEquals( 3, ids.length );
            assertTrue( ArrayUtils.contains( ids, relationshipIdBeforeSplit ) );
            assertTrue( ArrayUtils.contains( ids, relationshipIdAfterSplit ) );
            assertTrue( ArrayUtils.contains( ids, augmentedRelationshipId ) );

            stateController.combine();
        }
    }

    @Test
    public void augmentAllNodesFromStableAndNewStateOnRead()
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node1Before = databaseRule.createNode();
            Node node2Before = databaseRule.createNode();
            Node node3Before = databaseRule.createNode();
            stateController.split();

            Node node1After = databaseRule.createNode();
            Node node2After = databaseRule.createNode();

            TransactionState transactionState = getTransactionState();
            long augmentedNodeId = 12;
            PrimitiveLongResourceIterator nodesGetAll = transactionState.augmentNodesGetAll( iterator( augmentedNodeId ) );
            long[] ids = PrimitiveLongCollections.asArray( nodesGetAll );

            assertEquals( 6, ids.length );
            assertTrue( ArrayUtils.contains( ids, augmentedNodeId ) );
            assertTrue( ArrayUtils.contains( ids, node1Before.getId() ) );
            assertTrue( ArrayUtils.contains( ids, node2Before.getId() ) );
            assertTrue( ArrayUtils.contains( ids, node3Before.getId() ) );
            assertTrue( ArrayUtils.contains( ids, node1After.getId() ) );
            assertTrue( ArrayUtils.contains( ids, node2After.getId() ) );

            stateController.combine();
        }
    }

    @Test
    public void splitIndexUpdatesBetweenStates()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, "a" );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, "b" );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );
            TxState stableTxState = ((CombinedTxState) transactionState).getStableTxState();
            TxState currentTxState = ((CombinedTxState) transactionState).getCurrentTxState();

            checkIndexUpdates( node, descriptor, stableTxState );
            checkIndexUpdates( nodeNew, descriptor, currentTxState );

            stateController.combine();
        }
    }

    @Test
    public void indexUpdatesForRangeSeekByPrefixFromStableAndNewState()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, "aca" );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, "acb" );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );

            PrimitiveLongReadableDiffSets rangeSeekByPrefix = transactionState.indexUpdatesForRangeSeekByPrefix(
                    descriptor, "ac" );
            PrimitiveLongSet addedNodes = rangeSeekByPrefix.getAdded();
            assertEquals( 2, addedNodes.size() );
            assertTrue( addedNodes.contains( node.getId() ) );
            assertTrue( addedNodes.contains( nodeNew.getId() ) );

            stateController.combine();
        }
    }

    @Test
    public void indexUpdatesForRangeSeekByStringFromStableAndNewState()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, "aca" );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, "acb" );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );

            PrimitiveLongReadableDiffSets rangeSeekByPrefix =
                    transactionState.indexUpdatesForRangeSeekByString( descriptor, "aca", true, "acz", false );
            PrimitiveLongSet addedNodes = rangeSeekByPrefix.getAdded();
            assertEquals( 2, addedNodes.size() );
            assertTrue( addedNodes.contains( node.getId() ) );
            assertTrue( addedNodes.contains( nodeNew.getId() ) );

            stateController.combine();
        }
    }

    @Test
    public void indexUpdatesForRangeSeekByNumberFromStableAndNewState()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, 5 );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, 6 );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );

            PrimitiveLongReadableDiffSets rangeSeekByPrefix =
                    transactionState.indexUpdatesForRangeSeekByNumber( descriptor, 5, true, 6, true );
            PrimitiveLongSet addedNodes = rangeSeekByPrefix.getAdded();
            assertEquals( 2, addedNodes.size() );
            assertTrue( addedNodes.contains( node.getId() ) );
            assertTrue( addedNodes.contains( nodeNew.getId() ) );

            stateController.combine();
        }
    }

    @Test
    public void indexUpdatesForSeekFromStableAndNewState()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, "a" );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, "a" );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );

            PrimitiveLongReadableDiffSets rangeSeekByPrefix =
                    transactionState.indexUpdatesForSeek( descriptor, ValueTuple.of( "a" ) );
            PrimitiveLongSet addedNodes = rangeSeekByPrefix.getAdded();
            assertEquals( 2, addedNodes.size() );
            assertTrue( addedNodes.contains( node.getId() ) );
            assertTrue( addedNodes.contains( nodeNew.getId() ) );

            stateController.combine();
        }
    }

    @Test
    public void indexUpdatesForScanFromStableAndNewState()
    {
        Label marker = Label.label( "marker" );
        String property = "property";
        createIndex( marker, property );
        awaitForIndexes();

        try ( Transaction ignored = databaseRule.beginTx() )
        {
            TransactionStateController stateController = txStateController();
            Node node = databaseRule.createNode( marker );
            node.setProperty( property, "a" );

            stateController.split();

            Node nodeNew = databaseRule.createNode( marker );
            nodeNew.setProperty( property, "b" );

            IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( getLabelIdsByName( marker.name() ), getPropertyIdByName( property ) );
            TransactionState transactionState = getTransactionState();
            assertCombinedTxState( transactionState );

            PrimitiveLongReadableDiffSets rangeSeekByPrefix =
                    transactionState.indexUpdatesForScan( descriptor );
            PrimitiveLongSet addedNodes = rangeSeekByPrefix.getAdded();
            assertEquals( 2, addedNodes.size() );
            assertTrue( addedNodes.contains( node.getId() ) );
            assertTrue( addedNodes.contains( nodeNew.getId() ) );

            stateController.combine();
        }
    }

    private void checkIndexUpdates( Node node, IndexDescriptor descriptor, TxState txState )
    {
        PrimitiveLongReadableDiffSets updatesForScan = txState.indexUpdatesForScan( descriptor );
        PrimitiveLongSet added = updatesForScan.getAdded();
        assertEquals( 1, added.size() );
        assertTrue( added.contains( node.getId() ) );
    }

    private int getLabelIdsByName( String labelName )
    {
        try ( Statement statement = getKernelStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            return readOperations.labelGetForName( labelName );
        }
    }

    private int getPropertyIdByName( String name )
    {
        try ( Statement statement = getKernelStatement() )
        {
            ReadOperations readOperations = statement.readOperations();
            return readOperations.propertyKeyGetForName( name );
        }
    }

    private void createIndex( Label marker, String propertyKey )
    {
        try ( Transaction transaction = databaseRule.beginTx() )
        {
            databaseRule.schema().indexFor( marker ).on( propertyKey ).create();
            transaction.success();
        }
    }

    private void awaitForIndexes()
    {
        try ( Transaction ignored = databaseRule.beginTx() )
        {
            databaseRule.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
        }
    }

    private TransactionState getTransactionState()
    {
        KernelTransactionImplementation kernelTransaction = kernelTransaction();
        return kernelTransaction.txState();
    }

    private void assertCombinedTxState( TransactionState transactionState )
    {
        assertThat( transactionState, instanceOf( CombinedTxState.class ) );
    }

    private TransactionStateController txStateController()
    {
        KernelStatement statement = getKernelStatement();
        return statement.transactionStateController();
    }

    private KernelTransactionImplementation kernelTransaction()
    {
        KernelStatement statement = getKernelStatement();
        return statement.getTransaction();
    }

    private KernelStatement getKernelStatement()
    {
        DependencyResolver dependencyResolver = databaseRule.getDependencyResolver();
        return (KernelStatement) dependencyResolver.resolveDependency( ThreadToStatementContextBridge.class ).get();
    }

    private static class ArrayRelationshipVisitor extends RelationshipIterator.BaseIterator
    {
        private final long[] ids;
        private int position;

        ArrayRelationshipVisitor( long[] ids )
        {
            this.ids = ids;
        }

        @Override
        protected boolean fetchNext()
        {
            return ids.length > position && next( ids[position++] );
        }

        @Override
        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
        {
            visitor.visit( relationshipId, 1, 1L, 1L );
            return false;
        }
    }
}
