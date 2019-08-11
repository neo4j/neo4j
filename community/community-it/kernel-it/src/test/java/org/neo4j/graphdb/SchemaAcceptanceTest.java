/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.contains;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.createIndex;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getConstraints;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.inTx;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.isEmpty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.waitForIndex;

@ImpermanentDbmsExtension
class SchemaAcceptanceTest
{
    @Inject
    private GraphDatabaseService db;

    private Label label = Labels.MY_LABEL;
    private Label otherLabel = Labels.MY_OTHER_LABEL;
    private final String propertyKey = "my_property_key";
    private final String secondPropertyKey = "my_second_property_key";

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    @Test
    void addingAnIndexingRuleShouldSucceed()
    {
        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( db, label ), containsOnly( index ) );
        }
    }

    @Test
    void addingACompositeIndexingRuleShouldSucceed()
    {
        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey, secondPropertyKey );

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( db, label ), containsOnly( index ) );
        }
    }

    @Test
    void addingNamedIndexRuleShouldSucceed()
    {
        // When
        IndexDefinition index = createIndex( db, "MyIndex", label, propertyKey );

        // Then
        assertThat( index.getName(), is( "MyIndex" ) );
        assertThat( getIndexes( db, label ), inTx( db, containsOnly( index ) ) );
    }

    @Test
    void shouldThrowConstraintViolationIfAskedToIndexSamePropertyAndLabelTwiceInSameTx()
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.indexFor( label ).on( propertyKey ).create();
            try
            {
                schema.indexFor( label ).on( propertyKey ).create();
                fail( "Should not have validated" );
            }
            catch ( ConstraintViolationException e )
            {
                assertEquals( "There already exists an index :MY_LABEL(my_property_key).", e.getMessage() );
            }
            tx.commit();
        }
    }

    @Test
    void shouldThrowConstraintViolationIfAskedToIndexPropertyThatIsAlreadyIndexed()
    {
        // GIVEN
        Schema schema;
        try ( Transaction tx = db.beginTx() )
        {
            schema = db.schema();
            schema.indexFor( label ).on( propertyKey ).create();
            tx.commit();
        }

        // WHEN
        ConstraintViolationException caught = null;
        try ( Transaction tx = db.beginTx() )
        {
            schema.indexFor( label ).on( propertyKey ).create();
            tx.commit();
        }
        catch ( ConstraintViolationException e )
        {
            caught = e;
        }

        // THEN
        assertThat( caught, not( nullValue() ) );
    }

    @Test
    void shouldThrowConstraintViolationIfAskedToCreateCompoundConstraint()
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.constraintFor( label )
                    .assertPropertyIsUnique( "my_property_key" )
                    .assertPropertyIsUnique( "other_property" ).create();
            tx.commit();
            fail( "Should not be able to create constraint on multiple propertyKey keys" );
        }
        catch ( UnsupportedOperationException e )
        {
            assertThat( e.getMessage(), containsString( "can only create one unique constraint" ) );
        }
    }

    @Test
    void droppingExistingIndexRuleShouldSucceed()
    {
        // GIVEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // WHEN
        dropIndex( index );

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( db, label ), isEmpty() );
        }
    }

    @Test
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSameTransaction()
    {
        // GIVEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            index.drop();
            try
            {
                index.drop();
                fail( "Should not be able to drop index twice" );
            }
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getMessage(), containsString( "No such INDEX ON :MY_LABEL(my_property_key)." ) );
            }
            tx.commit();
        }

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( "Index should have been deleted", getIndexes( db, label ), not( contains( index ) ) );
        }
    }

    @Test
    void droppingNonExistingIndexShouldGiveHelpfulExceptionInSeparateTransactions()
    {
        // GIVEN
        IndexDefinition index = createIndex( db, label, propertyKey );
        dropIndex( index );

        // WHEN
        try
        {
            dropIndex( index );
            fail( "Should not be able to drop index twice" );
        }
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "No such INDEX ON :MY_LABEL(my_property_key)." ) );
        }

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( "Index should have been deleted", getIndexes( db, label ), not( contains( index ) ) );
        }
    }

    @Test
    void awaitingIndexComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // PASS
        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
        }
    }

    @Test
    void awaitingAllIndexesComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );
        createIndex( db, label, "other_property" );

        // PASS
        waitForIndex( db, index );
        try ( Transaction ignored = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
        }
    }

    @Test
    void shouldPopulateIndex()
    {
        // GIVEN
        Node node = createNode( db, propertyKey, "Neo", label );

        // create an index
        IndexDefinition index = createIndex( db, label, propertyKey );
        waitForIndex( db, index );

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( findNodesByLabelAndProperty( label, propertyKey, "Neo", db ), containsOnly( node ) );
        }
    }

    @Test
    void shouldRecreateDroppedIndex()
    {
        // GIVEN
        Node node = createNode( db, propertyKey, "Neo", label );

        // create an index
        IndexDefinition index = createIndex( db, label, propertyKey );
        waitForIndex( db, index );

        // delete the index right away
        dropIndex( index );

        // WHEN recreating that index
        createIndex( db, label, propertyKey );
        waitForIndex( db, index );

        try ( Transaction transaction = db.beginTx() )
        {
            // THEN it should exist and be usable
            assertThat( getIndexes( db, label ), contains( index ) );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, "Neo", db ), containsOnly( node ) );
            transaction.commit();
        }
    }

    @Test
    void shouldCreateUniquenessConstraint()
    {
        // WHEN
        ConstraintDefinition constraint = createUniquenessConstraint( label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.UNIQUENESS, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "Uniqueness constraint on :MY_LABEL (my_property_key)", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldCreateNamedUniquenessConstraint()
    {
        // When
        ConstraintDefinition constraint = createUniquenessConstraint( "MyConstraint", label, propertyKey );

        // Then
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.UNIQUENESS, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "MyConstraint", constraint.getName() );
            tx.commit();
        }
    }

    @Test
    void shouldGetConstraintByName()
    {
        ConstraintDefinition expectedConstraint = createUniquenessConstraint( "MyConstraint", label, propertyKey );

        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition actualConstraint = db.schema().getConstraintByName( "MyConstraint" );
            assertThat( actualConstraint, equalTo( expectedConstraint ) );
            tx.commit();
        }
    }

    @Test
    void shouldListAddedConstraintsByLabel()
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        createUniquenessConstraint( Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( db, label ), containsOnly( constraint1 ) );
        }
    }

    @Test
    void shouldListAddedConstraints()
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( Labels.MY_LABEL, propertyKey );
        ConstraintDefinition constraint2 = createUniquenessConstraint( Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( db ), containsOnly( constraint1, constraint2 ) );
        }
    }

    @Test
    void shouldDropUniquenessConstraint()
    {
        // GIVEN
        ConstraintDefinition constraint = createUniquenessConstraint( label, propertyKey );

        // WHEN
        dropConstraint( db, constraint );

        // THEN
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getConstraints( db, label ), isEmpty() );
        }
    }

    @Test
    void addingConstraintWhenIndexAlreadyExistsGivesNiceError()
    {
        // GIVEN
        createIndex( db, label, propertyKey );

        // WHEN
        try
        {
            createUniquenessConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "There already exists an index :MY_LABEL(my_property_key). A constraint cannot be created " +
                          "until the index has been dropped.", e.getMessage() );
        }
    }

    @Test
    void addingUniquenessConstraintWhenDuplicateDataExistsGivesNiceError()
    {
        // GIVEN
        try ( Transaction transaction = db.beginTx() )
        {
            db.createNode( label ).setProperty( propertyKey, "value1" );
            db.createNode( label ).setProperty( propertyKey, "value1" );
            transaction.commit();
        }

        // WHEN
        try
        {
            createUniquenessConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString(
                    "Unable to create CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT (my_label.my_property_key) IS UNIQUE" ) );
        }
    }

    @Test
    void addingConstraintWhenAlreadyConstrainedGivesNiceError()
    {
        // GIVEN
        createUniquenessConstraint( label, propertyKey );

        // WHEN
        try
        {
            createUniquenessConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals(
                    "Constraint already exists: CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT (my_label.my_property_key) " +
                    "IS UNIQUE",
                    e.getMessage() );
        }
    }

    @Test
    void addingIndexWhenAlreadyConstrained()
    {
        // GIVEN
        createUniquenessConstraint( label, propertyKey );

        // WHEN
        try
        {
            createIndex( db, label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "There is a uniqueness constraint on :MY_LABEL(my_property_key), so an index is already " +
                          "created that matches this.", e.getMessage() );
        }
    }

    @Test
    void addingIndexWhenAlreadyIndexed()
    {
        // GIVEN
        createIndex( db, label, propertyKey );

        // WHEN
        try
        {
            createIndex( db, label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "There already exists an index :MY_LABEL(my_property_key).", e.getMessage() );
        }
    }

    @Test
    void addedUncommittedIndexesShouldBeVisibleWithinTheTransaction()
    {
        // GIVEN
        IndexDefinition indexA = createIndex( db, label, "a" );
        createUniquenessConstraint( label, "b" );

        // WHEN
        try ( Transaction ignored = db.beginTx() )
        {
            assertThat( count( db.schema().getIndexes( label ) ), is( 2L ) );
            IndexDefinition indexC = db.schema().indexFor( label ).on( "c" ).create();
            // THEN
            assertThat( count( db.schema().getIndexes( label ) ), is( 3L ) );
            assertThat( db.schema().getIndexState( indexA ), is( Schema.IndexState.ONLINE ) );
            assertThat( db.schema().getIndexState( indexC ), is( Schema.IndexState.POPULATING ) );
            assertThat( db.schema().getIndexPopulationProgress( indexA ).getCompletedPercentage(), greaterThan( 0f ) );
            assertThat( db.schema().getIndexPopulationProgress( indexC ).getCompletedPercentage(), greaterThanOrEqualTo( 0f ) );
        }
    }

    @Test
    void indexNamesMustBeUnique()
    {
        createIndex( db, "MyIndex", label, propertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, "MyIndex", label, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( "MyIndex" ) );
    }

    @Test
    void indexNamesMustBeUniqueEvenWhenGenerated()
    {
        IndexDefinition index = createIndex( db, label, propertyKey ); // Index with generated name.
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, index.getName(), otherLabel, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( index.getName() ) );
    }

    @Test
    void indexNamesMustBeUniqueEvenWhenGenerated2()
    {
        IndexDefinition index = createIndex( db, "Index on :" + label.name() + " (" + propertyKey + ")", otherLabel, secondPropertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, label, propertyKey ) );
        assertThat( exception.getMessage(), containsString( index.getName() ) );
    }

    @Test
    void constraintNamesMustBeUnique()
    {
        createUniquenessConstraint( "MyConstraint", label, propertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createUniquenessConstraint( "MyConstraint", label, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( "MyConstraint" ) );
    }

    @Test
    void cannotCreateConstraintWithSameNameAsExistingIndex()
    {
        createIndex( db, "MySchema", label, propertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createUniquenessConstraint( "MySchema", label, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( "MySchema" ) );
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingIndexWithGeneratedName()
    {
        IndexDefinition index = createIndex( db, label, propertyKey ); // Index with generated name.
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, index.getName(), otherLabel, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( index.getName() ) );
    }

    @Test
    void cannotCreateConstraintWithSameNameAsExistingIndexWithGeneratedName()
    {
        IndexDefinition index = createIndex( db, label, propertyKey ); // Index with generated name.
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createUniquenessConstraint( index.getName(), otherLabel, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( index.getName() ) );
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingConstraint()
    {
        createUniquenessConstraint( "MySchema", label, propertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, "MySchema", label, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( "MySchema" ) );
    }

    @Test
    void cannotCreateIndexWithSameNameAsExistingConstraintWithGeneratedName()
    {
        ConstraintDefinition constraint = createUniquenessConstraint( label, propertyKey );
        ConstraintViolationException exception =
                assertThrows( ConstraintViolationException.class, () -> createIndex( db, constraint.getName(), label, secondPropertyKey ) );
        assertThat( exception.getMessage(), containsString( constraint.getName() ) );
    }

    @Test
    void uniquenessConstraintIndexesMustBeNamedAfterTheirConstraints()
    {
        createUniquenessConstraint( "MySchema", label, propertyKey );
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().getIndexByName( "MySchema" );
            assertTrue( index.isConstraintIndex() );
            assertTrue( index.isNodeIndex() );
            assertEquals( "MySchema", index.getName() );
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propertyKey ).withName( "MyIndex" ).create();
            IndexCreator creator = db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( "MyIndex" );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists an index called 'MyIndex'" ) );
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( label ).on( propertyKey ).create();
            IndexCreator creator = db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( index.getName() );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists an index called '" + index.getName() + "'" ) );
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated2()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = db.schema().indexFor( otherLabel ).on( secondPropertyKey )
                    .withName( "Index on :" + label.name() + " (" + propertyKey + ")" ).create();
            IndexCreator creator = db.schema().indexFor( label ).on( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists an index called '" + index.getName() + "'" ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "MyConstraint" ).create();
            ConstraintCreator creator = db.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( "MyConstraint" );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists with this name: MyConstraint" ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            ConstraintCreator creator = db.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( constraint.getName() );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists with this name: " + constraint.getName() ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated2()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = db.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey )
                    .withName( "Uniqueness constraint on :MY_LABEL (my_property_key)" ).create();
            ConstraintCreator creator = db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "already exists with this name: " + constraint.getName() ) );
            tx.commit();
        }
    }

    @Test
    void constraintAndIndexNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "MySchema" ).create();
            IndexCreator creator = db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( "MySchema" );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "MySchema" ) );
            tx.commit();
        }
    }

    @Test
    void indexAndConstraintNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( label ).on( propertyKey ).withName( "MySchema" ).create();
            ConstraintCreator creator = db.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( "MySchema" );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( "MySchema" ) );
            tx.commit();
        }
    }

    @Test
    void nodeKeyConstraintsMustNotAvailableInCommunityEdition()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator constraintCreator = db.schema().constraintFor( label ).assertPropertyIsNodeKey( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, constraintCreator::create );
            assertThat( exception.getMessage(), containsString( "Enterprise Edition" ) );
            tx.commit();
        }
    }

    @Test
    void propertyExistenceConstraintsMustNotBeAvailableInCommunityEdition()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator constraintCreator = db.schema().constraintFor( label ).assertPropertyExists( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, constraintCreator::create );
            assertThat( exception.getMessage(), containsString( "Enterprise Edition" ) );
            tx.commit();
        }
    }

    @Nested
    @ActorsExtension
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

        @RepeatedTest( 20 )
        void cannotCreateIndexesWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String indexName = "MyIndex";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    () -> db.schema().indexFor( label ).on( propertyKey ).withName( indexName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    () -> db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( indexName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @RepeatedTest( 20 )
        void cannotCreateConstraintsWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String constraintName = "MyConstraint";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    () -> db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( constraintName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    () -> db.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( constraintName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @RepeatedTest( 20 )
        void cannotCreateIndexesAndConstraintsWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String schemaName = "MySchema";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    () -> db.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( schemaName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    () -> db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( schemaName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @Test
        void droppingConstraintMustLockNameForIndexCreate() throws Exception
        {
            String schemaName = "MySchema";
            createUniquenessConstraint( schemaName, label, propertyKey );
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                tx.commit();
            }

            BinaryLatch afterFirstDropsConstraint = new BinaryLatch();
            BinaryLatch pauseFirst = new BinaryLatch();
            BinaryLatch beforeSecondCreatesIndex = new BinaryLatch();

            Future<Void> firstFuture = first.submit( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().getConstraintByName( schemaName ).drop();
                    afterFirstDropsConstraint.release();
                    pauseFirst.await();
                    tx.commit();
                }
            } );
            Future<Void> secondFuture = second.submit( () ->
            {
                afterFirstDropsConstraint.await();
                try ( Transaction tx = db.beginTx() )
                {
                    beforeSecondCreatesIndex.release();
                    IndexCreator indexCreator = db.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( schemaName );
                    indexCreator.create();
                    tx.commit();
                }
            } );

            first.untilWaitingIn( BinaryLatch.class.getMethod( "await") );
            beforeSecondCreatesIndex.await();
            second.untilWaitingIn( Object.class.getMethod( "wait", long.class ) );
            second.untilWaiting();
            pauseFirst.release();
            firstFuture.get();
            secondFuture.get();
            try ( Transaction tx = db.beginTx() )
            {
                assertFalse( db.schema().getConstraints().iterator().hasNext() );
                Iterator<IndexDefinition> indexes = db.schema().getIndexes().iterator();
                assertTrue( indexes.hasNext() );
                assertEquals( indexes.next().getName(), schemaName );
                assertFalse( indexes.hasNext() );
                tx.commit();
            }
        }

        private Callable<Void> schemaTransaction( ThrowingSupplier<Object, Exception> action )
        {
            return () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Object creator = action.get();
                    startLatch.await();
                    if ( creator instanceof IndexCreator )
                    {
                        ((IndexCreator) creator).create();
                    }
                    else if ( creator instanceof ConstraintCreator )
                    {
                        ((ConstraintCreator) creator).create();
                    }
                    else
                    {
                        fail( "Don't know how to create from " + creator );
                    }
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
            if ( firstThrowable == null )
            {
                assertThat( "The first transaction succeeded, so the second one should have failed.", secondThrowable,
                        instanceOf( ConstraintViolationException.class ) );
            }
            if ( secondThrowable == null )
            {
                assertThat( "The second transaction succeeded, so the first one should have failed.", firstThrowable,
                        instanceOf( ConstraintViolationException.class ) );
            }
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

    private void dropConstraint( GraphDatabaseService db, ConstraintDefinition constraint )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraint.drop();
            tx.commit();
        }
    }

    private ConstraintDefinition createUniquenessConstraint( Label label, String prop )
    {
        return createUniquenessConstraint( null, label, prop );
    }

    private ConstraintDefinition createUniquenessConstraint( String name, Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = db.schema().constraintFor( label );
            creator = creator.assertPropertyIsUnique( prop ).withName( name );
            ConstraintDefinition constraint = creator.create();
            tx.commit();
            return constraint;
        }
    }

    private void dropIndex( IndexDefinition index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            index.drop();
            tx.commit();
        }
    }

    private Node createNode( GraphDatabaseService db, String key, Object value, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, value );
            tx.commit();
            return node;
        }
    }
}
