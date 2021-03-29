/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

package org.neo4j.graphdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.schema.AnyTokens;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.schema.Schema.IndexState.ONLINE;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ImpermanentDbmsExtension( configurationCallback = "configure" )
public class TokenSchemaAcceptanceTest extends SchemaAcceptanceTest
{
    private final String propertyKey = "my_property_key";

    @ExtensionCallback
    @Override
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        super.configure( builder );
        // TODO this test extends SchemaAcceptanceTest to make sure SSTI doesn't break other schema operations
        // when cleaning up the flag this inheritance should be removed to not run same tests twice
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

    @BeforeEach
    void beforeEach()
    {
        // drop default indexes
        try ( var tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void addingTokenIndexRuleShouldSucceed( AnyTokens tokens )
    {
        // When
        IndexDefinition index = createIndex( db, tokens, null );

        // Then
        try ( var tx = db.beginTx() )
        {
            assertThat( tx.schema().getIndexes() ).containsOnly( index );
        }
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void addingNamedTokenIndexRuleShouldSucceed( AnyTokens tokens )
    {
        // When
        IndexDefinition index = createIndex( db, tokens, "MyIndex" );

        // Then
        assertThat( index.getName() ).isEqualTo( "MyIndex" );
        try ( var tx = db.beginTx() )
        {
            assertThat( tx.schema().getIndexes() ).containsOnly( index );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentTokenIndexExist( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( AnyTokens.ANY_LABELS ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( AnyTokens.ANY_LABELS ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        assertExpectedException( exception, expectedCause,
                "An equivalent index already exists, 'Index( id=",
                "name='name', type='GENERAL LOOKUP', schema=(:<any-labels>), indexProvider='token-1.0' )'." );
    }

    @ParameterizedTest
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfNameIsUsed( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( AnyTokens.ANY_LABELS ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( AnyTokens.ANY_RELATIONSHIP_TYPES ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( exception, expectedCause, expectedMessage );
    }

    @ParameterizedTest
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfNameIsUsedByPropertyIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( AnyTokens.ANY_LABELS ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( exception, expectedCause, expectedMessage );
    }

    @ParameterizedTest
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowOnPropertyIndexIfNameIsUsedByTokenIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( AnyTokens.ANY_LABELS ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( exception, expectedCause, expectedMessage );
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void droppingExistingIndexRuleShouldSucceed( AnyTokens token )
    {
        IndexDefinition index = createIndex( db, token, null );

        dropIndex( index );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.schema().getIndexes() ).isEmpty();
        }
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSameTransaction( AnyTokens token )
    {
        IndexDefinition index = createIndex( db, token, null );

        try ( Transaction tx = db.beginTx() )
        {
            index = tx.schema().getIndexByName( index.getName() );
            index.drop();
            ConstraintViolationException e = assertThrows( ConstraintViolationException.class, index::drop );
            assertThat( e ).hasMessageContaining( "Unable to drop index: Index does not exist: ");
            tx.commit();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( tx.schema().getIndexes() ).doesNotContain( index );
        }
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void awaitingTokenIndexComingOnlineWorks( AnyTokens token )
    {
        IndexDefinition index = createIndex( db, token, null );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( index, 2L, TimeUnit.MINUTES );

            assertThat( tx.schema().getIndexState( index ) ).isEqualTo( ONLINE );
        }
    }

    @ParameterizedTest
    @EnumSource( AnyTokens.class )
    void awaitingTokenIndexComingOnlineByNameWorks( AnyTokens token )
    {
        IndexDefinition index = createIndex( db, token, "my_index" );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "my_index", 2L, TimeUnit.MINUTES );

            assertThat( tx.schema().getIndexState( index ) ).isEqualTo( ONLINE );
        }
    }

    @Test
    void awaitingAllIndexesComingOnlineWorksWhenThereIsTokenIndex()
    {
        IndexDefinition index1 = createIndex( db, AnyTokens.ANY_LABELS, null );
        IndexDefinition index2 = createIndex( db, AnyTokens.ANY_RELATIONSHIP_TYPES, null );

        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2L, TimeUnit.MINUTES );

            assertThat( tx.schema().getIndexState( index1 ) ).isEqualTo( ONLINE );
            assertThat( tx.schema().getIndexState( index2 ) ).isEqualTo( ONLINE );
        }
    }

    @Test
    void tokenIndexCreatorThrowsOnProperty()
    {
        try ( var tx = db.beginTx() )
        {
            IndexCreator indexCreator = tx.schema().indexFor( AnyTokens.ANY_LABELS );

            assertThatThrownBy( () -> indexCreator.on( "property" ) ).isInstanceOf( ConstraintViolationException.class )
                                                                     .hasMessageContaining( "LOOKUP indexes doesn't support inclusion of property keys." );
        }
    }

    @Test
    void tokenIndexCreatorThrowsOnUnsupportedIndexTypes()
    {
        try ( var tx = db.beginTx() )
        {
            IndexCreator indexCreator = tx.schema().indexFor( AnyTokens.ANY_LABELS );

            assertThatThrownBy( () -> indexCreator.withIndexType( IndexType.BTREE ) )
                    .isInstanceOf( ConstraintViolationException.class )
                    .hasMessageContaining( "Only LOOKUP index type supported for token indexes." );
            assertThatThrownBy( () -> indexCreator.withIndexType( IndexType.FULLTEXT ) )
                    .isInstanceOf( ConstraintViolationException.class )
                    .hasMessageContaining( "Only LOOKUP index type supported for token indexes." );
        }
    }

    @Test
    @Disabled( "TODO No population yet" )
    void shouldPopulateTokenIndex()
    {
        // GIVEN
        Node node = createNode( db, propertyKey, "Neo", label );

        // create an index
        IndexDefinition index = createIndex( db, AnyTokens.ANY_LABELS, null );
        waitForIndex( db, index );

        // THEN
        try ( var tx = db.beginTx() )
        {
            assertThat( asSet( tx.findNodes( label ) ) ).containsOnly( node );
        }
    }

    @Disabled( "TODO Fails on system db recovery because it needs proper token index population" )
    @Test
    void crashDuringIndexPopulationOfConstraint() throws InterruptedException
    {
    }

    public static IndexDefinition createIndex( GraphDatabaseService db, AnyTokens tokens, String name )
    {
        IndexDefinition index;
        try ( var tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( tokens );
            if ( name != null )
            {
                creator = creator.withName( name );
            }
            index = creator.create();
            tx.commit();
        }
        waitForIndex( db, index );
        return index;
    }

    @Nested
    @ActorsExtension
    @ImpermanentDbmsExtension( configurationCallback = "configure" )
    class SchemaConcurrency
    {
        @Inject
        Actor first;
        @Inject
        Actor second;
        BinaryLatch startLatch;

        @BeforeEach
        void setUp()
        {
            startLatch = new BinaryLatch();
        }

        @ExtensionCallback
        void configure( TestDatabaseManagementServiceBuilder builder )
        {
            TokenSchemaAcceptanceTest.this.configure( builder );
        }

        @RepeatedTest( 20 )
        void cannotCreateTokenIndexesWithTheSameSchemaInConcurrentTransactions() throws Exception
        {
            Future<Void> firstFuture = first.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( AnyTokens.ANY_LABELS ).withName( "index-1" ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( AnyTokens.ANY_LABELS ).withName( "index-2" ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @RepeatedTest( 20 )
        void cannotCreateIndexesWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String indexName = "MyIndex";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( AnyTokens.ANY_LABELS ).withName( indexName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( AnyTokens.ANY_RELATIONSHIP_TYPES ).withName( indexName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        private Callable<Void> schemaTransaction( ThrowingFunction<Transaction, IndexCreator, Exception> action )
        {
            return () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    IndexCreator creator = action.apply(tx);
                    startLatch.await();
                    creator.create();
                    tx.commit();
                }
                return null;
            };
        }

        private void raceTransactions( Future<Void> firstFuture, Future<Void> secondFuture ) throws InterruptedException, NoSuchMethodException
        {
            first.untilWaitingIn( BinaryLatch.class.getMethod( "await") );
            second.untilWaitingIn( BinaryLatch.class.getMethod( "await") );
            startLatch.release();

            while ( !firstFuture.isDone() || !secondFuture.isDone() )
            {
                Thread.onSpinWait();
            }
        }

        private void assertOneSuccessAndOneFailure( Future<Void> firstFuture, Future<Void> secondFuture )
                throws InterruptedException
        {
            Throwable firstThrowable = getException( firstFuture );
            Throwable secondThrowable = getException( secondFuture );
            if ( firstThrowable == null && secondThrowable == null )
            {
                fail( "Both transactions completed successfully, when one of them should have thrown." );
            }
            Throwable error = firstThrowable != null ? firstThrowable : secondThrowable;
            // The most common exception is to notice the duplicate rule/name at transaction creation time, however there's a miniscule chance that
            // both transactions will progress a bit longer side by side and one of them instead tripping on a check that says that transactions
            // cannot commit if there has been a constraint created while the transaction was running.
            assertThat( error ).isInstanceOfAny( ConstraintViolationException.class );
        }

        private Throwable getException( Future<Void> future ) throws InterruptedException
        {
            try
            {
                future.get();
                return null;
            }
            catch ( ExecutionException e )
            {
                return e.getCause();
            }
        }
    }
}
