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
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.ArrayUtil;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TransactionStateController;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.RelationshipDataExtractor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

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
        FeatureToggles.set( TransactionStatesContainer.class, "multiState", true );
    }

    @After
    public void tearDown() throws Exception
    {
        FeatureToggles.clear( TransactionStatesContainer.class, "multiState" );
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

    private TransactionState getTransactionState()
    {
        KernelTransactionImplementation kernelTransaction = kernelTransaction();
        return kernelTransaction.txState();
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
