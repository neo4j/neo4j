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

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.exceptions.schema.ConstraintVerificationFailedKernelException;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.IntegrityValidator;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContextSupplier;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.OtherThreadRule;
import org.neo4j.test.TestLabels;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * Non deterministic, but also non-flaky test for trying to violate uniqueness constraint at the same time
 * that constraint is being created. The offending transaction gets applied via
 * {@link org.neo4j.kernel.impl.api.TransactionCommitProcess#commit(TransactionRepresentation, org.neo4j.kernel.impl.locking.LockGroup, org.neo4j.kernel.impl.transaction.tracing.CommitEvent)} }.
 *
 * Overall scenario:
 * - Node N1 exists with label/property pair PL
 * - Transaction T1 starts on slave and creates N2 with same label/property pair PL
 * - Transaction T2 creates a uniqueness constraint on master, at least creates the index and starts populating that.
 * - T1 commits on master
 * - T2 finishes the index population and makes the constraint ONLINE.
 *
 * There are checks in place to guard for this problem. Either T1 should rollback, or T2 should rollback,
 * depending on which one wins the race of committing first. But there is a possibility that both succeed
 * and the constraint is violated.
 *
 * This test runs for X seconds and during this time will try it's best to break the above guards.
 * Any failure caused is very likely an issue in the product. After the particular issue it was written for
 * is fixed it's still useful to have since it is able to catch a broader range of issues than a more
 * specific test would ever be able to.
 *
 * @author Mattias Persson
 */
public class UniquenessConstraintViolationRaceIT
{

    private final Random random = new Random();
    public final @Rule DatabaseRule dbr = new ImpermanentDatabaseRule();
    public final @Rule OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    @Test
    public void shouldTriggerTheRace() throws Exception
    {
        // GIVEN
        Label label = TestLabels.LABEL_ONE;
        String key = "name";
        Object value = "me";

        long endTime = currentTimeMillis() + SECONDS.toMillis( 5 );
        while ( currentTimeMillis() < endTime )
        {
            clearDatabase();

            int noiseLevel = random.nextInt( 100 );
            tryToReproduceTheIssue( label, key, value, noiseLevel );
        }
    }

    private void clearDatabase()
    {
        GraphDatabaseService db = dbr.getGraphDatabaseService();

        // Delete constraints
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints() )
            {
                constraint.drop();
                tx.success();
            }
        }

        // Delete nodes
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                node.delete();
            }
            tx.success();
        }
    }

    private void tryToReproduceTheIssue( Label label, String key, Object value,
            int amountOfNoise ) throws InterruptedException, IOException
    {
        GraphDatabaseAPI db = dbr.getGraphDatabaseAPI();
        createNoiseData( db, label, key, amountOfNoise );
        createSignatureNode( db, label, key, value );

        // SCENARIO
        // T1 starts transaction
        TransactionRepresentation offendingTransaction = offendingTransactionData( db, label, key, value );
        Future<Void> constraintFuture = null;
        Throwable t1Failure = null;
        Throwable t2Failure = null;

        // T2 starts and creates uniqueness constraint in another transaction
        constraintFuture = t2.execute( createUniquenessConstraint( db, label, key ) );

        // In the meantime T1 creates (injects) the offending node
        // Apparently we need to do this in a transaction. I think it's merely a mistake that we need that.
        try ( Transaction tx = db.beginTx();
              LockGroup locks = new LockGroup() )
        {
            // And tries to commit the similar node
            db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class ).commit(
                    offendingTransaction, locks, CommitEvent.NULL );
            tx.success();
        }
        catch ( org.neo4j.kernel.api.exceptions.TransactionFailureException e )
        {
            t1Failure = e;
        }

        try
        {
            constraintFuture.get();
        }
        catch ( ExecutionException e )
        {
            t2Failure = e.getCause();
            if ( t2Failure instanceof ConstraintViolationException )
            {   // This is OK, we can get that as part of building the index where we see that there are
                // violating data.
            }
            else if ( t2Failure instanceof TransactionFailureException )
            {   // This is OK, we can get that a bit later in the process, where we have built the index,
                // flipped to tentative index proxy and is about to commit (in the prepare phase to be exact).
                // Some transaction coming in the back door and being applied can have violated the constraint
                // as this point. Just make sure the transaction failed for the right reason.
                Exceptions.contains( t2Failure, ConstraintVerificationFailedKernelException.class );
            }
        }

        // THEN
        if ( t1Failure == null && t2Failure == null )
        {   // T1 and T2 managed to commit. We now have data that violates the constraint.
            // Assertions asserting our assumptions
            assertConstraintExistence( db, true, label, key );
            assertNumberOfSignatureNodes( db, 2, label, key, value );
            fail( "Both were successful. The db now has data that violates the constraint, "
                    + "I just verified that's the case actually" );
        }
        else if ( t1Failure != null && t2Failure != null )
        {   // Neither T1 nor T2 managed to commit. This could be an issue with the test or something.
            assertConstraintExistence( db, false, label, key );
            assertNumberOfSignatureNodes( db, 1, label, key, value );
            fail( "No transaction managed to commit, please check the test" );
        }
        // OK outcome
        else if ( t1Failure != null )
        {   // T1 failed to create the similar node and T2 created the constraint, which is now online.
            assertConstraintExistence( db, true, label, key );
            assertNumberOfSignatureNodes( db, 1, label, key, value );
            assertAbilityToCreateSignatureNode( db, label, key, value, false );
        }
        // OK outcome
        else
        {   // T2 failed to create the constraint and T1 managed to create the similar node.
            assertConstraintExistence( db, false, label, key );
            assertNumberOfSignatureNodes( db, 2, label, key, value );
            assertAbilityToCreateSignatureNode( db, label, key, value, true );
        }
    }

    private void assertAbilityToCreateSignatureNode( GraphDatabaseAPI db, Label label, String key,
            Object value, boolean expectedAbleTo )
    {
        boolean success = false;
        try
        {
            createSignatureNode( db, label, key, value );
            success = true;
        }
        catch ( Exception e )
        {   // TODO narrow down exception type
        }

        assertEquals( "Should " + (expectedAbleTo ? "" : "not") +
                " have been able to create signature node at this point", expectedAbleTo, success );
    }

    private void createNoiseData( GraphDatabaseAPI db, Label label, String key, int amountOfNoise )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < amountOfNoise; i++ )
            {
                db.createNode( label ).setProperty( "name", "noise-" + i );
            }
            tx.success();
        }
    }

    private void assertConstraintExistence( GraphDatabaseAPI db, boolean expectedExists,
            Label label, String key )
    {
        boolean exists = false;
        try ( Transaction tx = db.beginTx() )
        {
            for ( ConstraintDefinition constraint : db.schema().getConstraints( label ) )
            {
                if ( constraint.isConstraintType( ConstraintType.UNIQUENESS )
                        && label.name().equals( constraint.getLabel().name() )
                        && key.equals( single( constraint.getPropertyKeys() ) ) )
                {
                    exists = true;
                }
            }
            tx.success();
        }

        assertEquals( "Expected constraint to " + (expectedExists ? "exist" : "not exist"), expectedExists, exists );
    }

    private void assertNumberOfSignatureNodes( GraphDatabaseService db, int expectedCount, final Label label,
            final String key, final Object value )
    {
        int count = 0;
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                if ( node.hasLabel( label ) && value.equals( node.getProperty( key, null ) ) )
                {
                    count++;
                }
            }
            tx.success();
        }

        assertEquals( "Unexpected number of nodes with label:" + label + " and property " + key + "=" + value,
                expectedCount, count );
    }

    private TransactionRepresentation offendingTransactionData( GraphDatabaseAPI db, Label label,
            String key, Object value ) throws IOException
    {
        DependencyResolver dependencyResolver = db.getDependencyResolver();
        NeoStore neoStore = dependencyResolver.resolveDependency( NeoStoreProvider.class ).evaluate();
        IndexingService indexingService = dependencyResolver.resolveDependency( IndexingService.class );
        NeoStoreTransactionContextSupplier supplier = new NeoStoreTransactionContextSupplier( neoStore );
        NeoStoreTransactionContext context = new NeoStoreTransactionContext( supplier, neoStore );
        TransactionRecordState recordState = new TransactionRecordState( neoStore,
                new IntegrityValidator( neoStore, indexingService ), context );
        long noNext = Record.NO_NEXT_RELATIONSHIP.intValue();

        NodeRecord nodeRecord = new NodeRecord( neoStore.getNodeStore().nextId(), false, noNext, 0 /* set below */ );
        nodeRecord.setInUse( true );
        PropertyRecord propertyRecord = new PropertyRecord( neoStore.getPropertyStore().nextId(), nodeRecord );
        propertyRecord.addPropertyBlock( encodeProperty( neoStore, key, value ) );
        nodeRecord.setNextProp( propertyRecord.getId() );
        propertyRecord.setInUse( true );
        NodeLabelsField.parseLabelsField( nodeRecord ).add( tokenId( neoStore.getLabelTokenStore(), label.name() ),
                neoStore.getNodeStore(), neoStore.getNodeStore().getDynamicLabelStore() );

        NodeCommand nodeCommand = new NodeCommand();
        nodeCommand.init( new NodeRecord( nodeRecord.getId() ), nodeRecord );
        PropertyCommand propertyCommand = new PropertyCommand();
        propertyCommand.init( new PropertyRecord( propertyRecord.getId() ), propertyRecord );

        PhysicalTransactionRepresentation tx =
                new PhysicalTransactionRepresentation( asList( nodeCommand, propertyCommand ) );
        tx.setHeader( new byte[0], 0, 0, currentTimeMillis(), BASE_TX_ID, currentTimeMillis(), 0 );
        return tx;
    }

    private PropertyBlock encodeProperty( NeoStore neoStore, String key, Object value )
    {
        PropertyBlock property = new PropertyBlock();
        neoStore.getPropertyStore().encodeValue( property,
                tokenId( neoStore.getPropertyStore().getPropertyKeyTokenStore(), key ), value );
        return property;
    }

    private int tokenId( TokenStore<?> tokenStore, String name )
    {
        for ( Token token : tokenStore.getTokens( 100 ) )
        {
            if ( token.name().equals( name ) )
            {
                return token.id();
            }
        }
        throw new IllegalArgumentException( "No token '" + name + "' found in " + tokenStore );
    }

    private WorkerCommand<Void, Void> createUniquenessConstraint( final GraphDatabaseService db,
            final Label label, final String key )
    {
        return new WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state ) throws Exception
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().constraintFor( label ).assertPropertyIsUnique( key ).create();
                    tx.success();
                }
                return null;
            }
        };
    }

    private void createSignatureNode( GraphDatabaseService db, Label label, String key, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( label ).setProperty( key, value );
            tx.success();
        }
    }
}
