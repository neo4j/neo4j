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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.graphdb.schema.IndexSettingImpl;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.AlreadyIndexedException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintWithNameAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.IndexWithNameAlreadyExistsException;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.schema.IndexType.BTREE;
import static org.neo4j.graphdb.schema.IndexType.FULLTEXT;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.contains;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.containsOnly;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.createIndex;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getConstraints;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.getIndexes;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.isEmpty;
import static org.neo4j.test.mockito.matcher.Neo4jMatchers.waitForIndex;

@ImpermanentDbmsExtension
class SchemaAcceptanceTest extends SchemaAcceptanceTestBase
{
    @Inject
    private GraphDatabaseService db;

    private final Label label = Label.label( "MY_LABEL" );
    private final Label otherLabel = Label.label( "MY_OTHER_LABEL" );
    private final RelationshipType relType = RelationshipType.withName( "MY_REL_TYPE" );
    private final RelationshipType otherRelType = RelationshipType.withName( "MY_OTHER_REL_TYPE" );
    private final String propertyKey = "my_property_key";
    private final String secondPropertyKey = "my_second_property_key";

    @Test
    void addingAnIndexingRuleShouldSucceed()
    {
        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), containsOnly( index ) );
        }
    }

    @Test
    void addingACompositeIndexingRuleShouldSucceed()
    {
        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey, secondPropertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), containsOnly( index ) );
        }
    }

    @Test
    void addingNamedIndexRuleShouldSucceed()
    {
        // When
        IndexDefinition index = createIndex( db, "MyIndex", label, propertyKey );

        // Then
        assertThat( index.getName(), is( "MyIndex" ) );
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( getIndexes( transaction, label ), containsOnly( index ) );
        }
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentIndexExist( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        String expectedMessage = "An equivalent index already exists, 'Index( 1, 'name', GENERAL BTREE, :MY_LABEL(my_property_key), native-btree-1.0 )'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfEquivalentUniquenessConstraintExist( SchemaTxStrategy txStrategy )
    {

        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<EquivalentSchemaRuleAlreadyExistsException> expectedCause = EquivalentSchemaRuleAlreadyExistsException.class;
        String expectedMessage = "An equivalent constraint already exists, 'Constraint( UNIQUE, :MY_LABEL(my_property_key) )'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyIndexedWhenCreatingIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyIndexedException> expectedCause = AlreadyIndexedException.class;
        String expectedMessage = "There already exists an index :MY_LABEL(my_property_key).";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyIndexedWhenCreatingUniquenessConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyIndexedException> expectedCause = AlreadyIndexedException.class;
        String expectedMessage = "There already exists an index :MY_LABEL(my_property_key). A constraint cannot be created until the index has been dropped.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "There is a uniqueness constraint on :MY_LABEL(my_property_key), so an index is already created that matches this.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfSchemaAlreadyUniquenessConstrainedWhenCreatingUniquenessConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "otherName" ).create(),
                ConstraintViolationException.class );
        Class<AlreadyConstrainedException> expectedCause = AlreadyConstrainedException.class;
        String expectedMessage = "Constraint already exists: Constraint( UNIQUE, :MY_LABEL(my_property_key) )";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfIndexWithNameExistsWhenCreatingIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfIndexWithNameExistsWhenCreatingUniquenessConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.indexFor( label ).on( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<IndexWithNameAlreadyExistsException> expectedCause = IndexWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists an index called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfConstraintWithNameExistsWhenCreatingIndex( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.indexFor( label ).on( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @ParameterizedTest()
    @EnumSource( SchemaTxStrategy.class )
    void shouldThrowIfConstraintWithNameExistsWhenCreatingUniquenessConstraint( SchemaTxStrategy txStrategy )
    {
        final ConstraintViolationException exception = txStrategy.execute( db,
                schema -> schema.constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "name" ).create(),
                schema1 -> schema1.constraintFor( label ).assertPropertyIsUnique( secondPropertyKey ).withName( "name" ).create(),
                ConstraintViolationException.class );
        Class<ConstraintWithNameAlreadyExistsException> expectedCause = ConstraintWithNameAlreadyExistsException.class;
        String expectedMessage = "There already exists a constraint called 'name'.";
        assertExpectedException( expectedCause, expectedMessage, exception );
    }

    @Test
    void shouldThrowConstraintViolationIfAskedToCreateCompoundConstraint()
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = tx.schema();
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
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getIndexes( tx, label ), isEmpty() );
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
            index = tx.schema().getIndexByName( index.getName() );
            index.drop();
            try
            {
                index.drop();
                fail( "Should not be able to drop index twice" );
            }
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getMessage(), containsString( "Unable to drop index: Index does not exist: " +
                        "Index( 1, 'index_a0d2924', GENERAL BTREE, :MY_LABEL(my_property_key), native-btree-1.0 )" ) );
            }
            tx.commit();
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( "Index should have been deleted", getIndexes( tx, label ), not( contains( index ) ) );
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
        catch ( Exception e )
        {
            assertThat( e.getMessage(), containsString( "No index found with the name 'index_a0d2924'." ) );
        }

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( "Index should have been deleted", getIndexes( tx, label ), not( contains( index ) ) );
        }
    }

    @Test
    void awaitingIndexComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // PASS
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, tx.schema().getIndexState( index ) );
        }
    }

    @Test
    void awaitingIndexComingOnlineByNameWorks()
    {
        IndexDefinition index = createIndex( db, "my_index", label, propertyKey );

        // PASS
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexOnline( "my_index", 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, tx.schema().getIndexState( index ) );
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
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, tx.schema().getIndexState( index ) );
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
            assertThat( findNodesByLabelAndProperty( label, propertyKey, "Neo", db, transaction ), containsOnly( node ) );
        }
    }

    @Test
    void recreatingDroppedIndexMustProduceNewDefinition()
    {
        // GIVEN
        Node node = createNode( db, propertyKey, "Neo", label );

        // create an index
        IndexDefinition index = createIndex( db, label, propertyKey );
        waitForIndex( db, index );

        // delete the index right away
        dropIndex( index );

        // WHEN recreating that index
        IndexDefinition newIndex = createIndex( db, label, propertyKey );
        try
        {
            waitForIndex( db, index );
        }
        catch ( NotFoundException e )
        {
            assertThat( e.getMessage(), containsString( "No index was found" ) );
        }
        waitForIndex( db, newIndex );

        try ( Transaction transaction = db.beginTx() )
        {
            // THEN it should exist and be usable
            assertThat( getIndexes( transaction, label ), contains( index ) );
            assertThat( findNodesByLabelAndProperty( label, propertyKey, "Neo", db, transaction ), containsOnly( node ) );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
            assertEquals( ConstraintType.UNIQUENESS, constraint.getConstraintType() );
            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), Iterables.asSet( constraint.getPropertyKeys() ) );
            assertEquals( "constraint_c8a3b28f", constraint.getName() );
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
            constraint = tx.schema().getConstraintByName( constraint.getName() );
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
            ConstraintDefinition actualConstraint = tx.schema().getConstraintByName( "MyConstraint" );
            assertThat( actualConstraint, equalTo( expectedConstraint ) );
            tx.commit();
        }
    }

    @Test
    void shouldListAddedConstraintsByLabel()
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        createUniquenessConstraint( otherLabel, propertyKey );

        // WHEN THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getConstraints( tx, label ), containsOnly( constraint1 ) );
        }
    }

    @Test
    void shouldListAddedConstraints()
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        ConstraintDefinition constraint2 = createUniquenessConstraint( otherLabel, propertyKey );

        // WHEN THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getConstraints( tx ), containsOnly( constraint1, constraint2 ) );
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
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( getConstraints( tx, label ), isEmpty() );
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
            transaction.createNode( label ).setProperty( propertyKey, "value1" );
            transaction.createNode( label ).setProperty( propertyKey, "value1" );
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
    void addedUncommittedIndexesShouldBeVisibleWithinTheTransaction()
    {
        // GIVEN
        IndexDefinition indexA = createIndex( db, label, "a" );
        createUniquenessConstraint( label, "b" );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.schema().getIndexes( label ) ), is( 2L ) );
            IndexDefinition indexC = tx.schema().indexFor( label ).on( "c" ).create();
            // THEN
            assertThat( count( tx.schema().getIndexes( label ) ), is( 3L ) );
            assertThat( tx.schema().getIndexState( indexA ), is( Schema.IndexState.ONLINE ) );
            assertThat( tx.schema().getIndexState( indexC ), is( Schema.IndexState.POPULATING ) );
            assertThat( tx.schema().getIndexPopulationProgress( indexA ).getCompletedPercentage(), greaterThan( 0f ) );
            assertThat( tx.schema().getIndexPopulationProgress( indexC ).getCompletedPercentage(), greaterThanOrEqualTo( 0f ) );
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
        IndexDefinition index = createIndex( db, "index_a0d2924", otherLabel, secondPropertyKey );
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
            IndexDefinition index = tx.schema().getIndexByName( "MySchema" );
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
            final String indexName = "MyIndex";
            tx.schema().indexFor( label ).on( propertyKey ).withName( indexName ).create();
            IndexCreator creator = tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( indexName );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( alreadyExistsIndexMessage( indexName ) ) );
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label ).on( propertyKey ).create();
            IndexCreator creator = tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( index.getName() );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( alreadyExistsIndexMessage( index.getName() ) ) );
            tx.commit();
        }
    }

    @Test
    void indexNamesInTransactionStateMustBeUniqueEvenWhenGenerated2()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( otherLabel ).on( secondPropertyKey )
                    .withName( "index_a0d2924" ).create();
            IndexCreator creator = tx.schema().indexFor( label ).on( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( alreadyExistsIndexMessage( index.getName() ) ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "MyConstraint" ).create();
            ConstraintCreator creator = tx.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( "MyConstraint" );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( thereAlreadyExistsConstraintMessage( "MyConstraint" ) ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            ConstraintCreator creator = tx.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( constraint.getName() );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( thereAlreadyExistsConstraintMessage( constraint.getName() ) ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesInTransactionStateMustBeUniqueEvenWhenGenerated2()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey )
                    .withName( "constraint_c8a3b28f" ).create();
            ConstraintCreator creator = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, creator::create );
            assertThat( exception.getMessage(), containsString( thereAlreadyExistsConstraintMessage( constraint.getName() ) ) );
            tx.commit();
        }
    }

    @Test
    void constraintAndIndexNamesInTransactionStateMustBeUnique()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( "MySchema" ).create();
            IndexCreator creator = tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( "MySchema" );
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
            tx.schema().indexFor( label ).on( propertyKey ).withName( "MySchema" ).create();
            ConstraintCreator creator = tx.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( "MySchema" );
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
            ConstraintCreator constraintCreator = tx.schema().constraintFor( label ).assertPropertyIsNodeKey( propertyKey );
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
            ConstraintCreator constraintCreator = tx.schema().constraintFor( label ).assertPropertyExists( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, constraintCreator::create );
            assertThat( exception.getMessage(), containsString( "Enterprise Edition" ) );
            tx.commit();
        }
    }

    @Test
    void propertyExistenceConstraintsOnRelationshipMustNotBeAvailableInCommunityEdition()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator constraintCreator = tx.schema().constraintFor( relType ).assertPropertyExists( propertyKey );
            ConstraintViolationException exception = assertThrows( ConstraintViolationException.class, constraintCreator::create );
            assertThat( exception.getMessage(), containsString( "Enterprise Edition" ) );
            tx.commit();
        }
    }

    @Test
    void indexNamesCannotContainBackTicks()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label ).withName( "a`b" ).on( propertyKey );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.schema().getIndexes() ), is( 0L ) );
            assertThat( count( tx.schema().getConstraints() ), is( 0L ) );
            tx.commit();
        }
    }

    @Test
    void constraintNamesCannotContainBackTicks()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).withName( "a`b" ).assertPropertyIsUnique( propertyKey );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( tx.schema().getIndexes() ), is( 0L ) );
            assertThat( count( tx.schema().getConstraints() ), is( 0L ) );
            tx.commit();
        }
    }

    @Test
    void mustCreateFullTextIndexBySettingIndexType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label ).on( propertyKey ).withIndexType( IndexType.FULLTEXT );
            IndexDefinition definition = creator.create();
            assertEquals( IndexType.FULLTEXT, definition.getIndexType() );

            IndexProviderDescriptor provider = ((IndexDefinitionImpl) definition).getIndexReference().getIndexProvider();
            assertEquals( provider, FulltextIndexProviderFactory.DESCRIPTOR );
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToGetIndexConfig()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label ).on( propertyKey ).withName( "my_index" ).create();
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertNotNull( config );
            assertTrue( config.containsKey( IndexSettingImpl.SPATIAL_CARTESIAN_MIN ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "my_index" );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertNotNull( config );
            assertTrue( config.containsKey( IndexSettingImpl.SPATIAL_CARTESIAN_MIN ) );
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToGetFullTextIndexConfig()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label ).withName( "my_index" ).on( propertyKey ).withIndexType( IndexType.FULLTEXT ).create();
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertNotNull( config );
            assertTrue( config.containsKey( IndexSettingImpl.FULLTEXT_ANALYZER ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "my_index" );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertNotNull( config );
            assertTrue( config.containsKey( IndexSettingImpl.FULLTEXT_ANALYZER ) );
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSetFullTextIndexConfig()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label ).withName( "my_index" ).on( propertyKey )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of(
                            IndexSettingImpl.FULLTEXT_ANALYZER, "swedish",
                            IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, true ) )
                    .create();
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertEquals( "swedish", config.get( IndexSettingImpl.FULLTEXT_ANALYZER ) );
            assertEquals( true, config.get( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "my_index" );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertEquals( "swedish", config.get( IndexSettingImpl.FULLTEXT_ANALYZER ) );
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSetBtreeIndexConfig()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label ).withName( "my_index" ).on( propertyKey )
                    .withIndexConfiguration( Map.of(
                            IndexSettingImpl.SPATIAL_CARTESIAN_MAX, new double[] {200.0, 200.0},
                            IndexSettingImpl.SPATIAL_WGS84_MIN, new double[] {-90.0, -90.0} ) )
                    .create();
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertArrayEquals( new double[] {200.0, 200.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_CARTESIAN_MAX ) );
            assertArrayEquals( new double[] {-90.0, -90.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_WGS84_MIN ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "my_index" );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertArrayEquals( new double[] {200.0, 200.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_CARTESIAN_MAX ) );
            assertArrayEquals( new double[] {-90.0, -90.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_WGS84_MIN ) );
            tx.commit();
        }
    }

    /**
     * This is the example used in {@link IndexSettingImpl}.
     */
    @Test
    void indexConfigurationExample()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( "Email" ) ).on( "from" ).on( "to" ).on( "cc" ).on( "bcc" )
                    .withName( "email-addresses" )
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSetting.fulltext_Analyzer(), "email" ) )
                    .create();
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "email-addresses" );
            assertThat( index.getPropertyKeys(), containsInAnyOrder( "from", "to", "cc", "bcc" ) );
            assertThat( index.getIndexConfiguration().get( IndexSetting.fulltext_Analyzer() ), is( "email" ) );
            tx.commit();
        }
    }

    @Test
    void indexSettingValuesMustHaveCorrectType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexCreator creator = tx.schema().indexFor( label ).withName( "my_index" ).on( propertyKey );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.FULLTEXT_ANALYZER, 1 ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.FULLTEXT_ANALYZER, true ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, "true" ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.FULLTEXT_EVENTUALLY_CONSISTENT, 1 ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexConfiguration( Map.of( IndexSettingImpl.SPATIAL_CARTESIAN_MAX, "1" ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexConfiguration( Map.of( IndexSettingImpl.SPATIAL_CARTESIAN_MAX, 1 ) )
                    .create() );
            assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexConfiguration( Map.of( IndexSettingImpl.SPATIAL_CARTESIAN_MAX, 1.0 ) )
                    .create() );
            tx.commit();
        }
    }

    @Test
    void indexSettingsWithNonsensicalValuesMustBeRejected()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Exception e;
            IndexCreator creator = tx.schema().indexFor( label ).withName( "my_index" ).on( propertyKey );
            e = assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexType( IndexType.FULLTEXT )
                    .withIndexConfiguration( Map.of( IndexSettingImpl.FULLTEXT_ANALYZER, "analyzer that does not exist" ) )
                    .create() );
            assertThat( e.getMessage(), containsString( "'analyzer that does not exist'" ) );

            e = assertThrows( IllegalArgumentException.class, () -> creator
                    .withIndexConfiguration( Map.of( IndexSettingImpl.SPATIAL_CARTESIAN_MAX, new double[] {100.0, 10.0, 1.0} ) )
                    .create() );
            assertThat( e.getMessage(), containsString( "Invalid spatial index settings" ) );

            tx.commit();
        }
    }

    @Test
    void creatingFullTextIndexOnMultipleLabelsMustBePossible()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( label, otherLabel ).on( propertyKey )
                    .withIndexType( IndexType.FULLTEXT ).withName( "index" ).create();
            assertThat( index.getLabels(), containsInAnyOrder( label, otherLabel ) );
            assertTrue( index.isMultiTokenIndex() );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "index" );
            List<String> labelNames = new ArrayList<>();
            index.getLabels().forEach( label -> labelNames.add( label.name() ) );
            assertThat( labelNames, containsInAnyOrder( label.name(), otherLabel.name() ) );
            assertTrue( index.isMultiTokenIndex() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingBtreeIndexWithZeroLabels()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().indexFor( new Label[0] ).on( propertyKey ).create() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingBtreeIndexWithMoreThanOneLabel()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().indexFor( label, otherLabel ).on( propertyKey ).create() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingFullTextIndexWithZeroLabels()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class,
                    () -> tx.schema().indexFor( new Label[0] ).on( propertyKey ).withIndexType( IndexType.FULLTEXT ).create() );
            tx.commit();
        }
    }

    @Test
    void creatingFullTextIndexOnRelationshipTypeMustBePossible()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( relType ).on( propertyKey ).withIndexType( IndexType.FULLTEXT ).withName( "index" ).create();
            assertTrue( index.isRelationshipIndex() );
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( relType ) );
            assertThat( index.getIndexType(), is( IndexType.FULLTEXT ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "index" );
            assertTrue( index.isRelationshipIndex() );
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( relType ) );
            assertThat( index.getIndexType(), is( IndexType.FULLTEXT ) );
            tx.commit();
        }
    }

    @Test
    void creatingMultiTokenFullTextIndexOnRelationshipTypesMustBePossible()
    {
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().indexFor( relType, otherRelType )
                    .on( propertyKey ).withIndexType( IndexType.FULLTEXT ).withName( "index" ).create();
            assertTrue( index.isRelationshipIndex() );
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( relType, otherRelType ) );
            assertThat( index.getIndexType(), is( IndexType.FULLTEXT ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "index" );
            assertTrue( index.isRelationshipIndex() );
            assertThat( index.getRelationshipTypes(), containsInAnyOrder( relType, otherRelType ) );
            assertThat( index.getIndexType(), is( IndexType.FULLTEXT ) );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingFullTextIndexOnZeroRelationshipTypes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class,
                    () -> tx.schema().indexFor( new RelationshipType[0] ).on( propertyKey ).withIndexType( IndexType.FULLTEXT ).create() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingBtreeIndexOnRelationshipType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().indexFor( relType ).on( propertyKey ).create() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingBtreeIndexOnZeroRelationshipTypes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().indexFor( new RelationshipType[0] ).on( propertyKey ).create() );
            tx.commit();
        }
    }

    @Test
    void mustThrowWhenCreatingBtreeIndexOnMultipleRelationshipTypes()
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertThrows( IllegalArgumentException.class, () -> tx.schema().indexFor( relType, otherRelType ).on( propertyKey ).create() );
            tx.commit();
        }
    }

    @Test
    void uniquenessConstraintIndexesAreBtreeIndexTypeByDefault()
    {
        String name;
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            name = constraint.getName();
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
        }
    }

    @Test
    void mustBeAbleToCreateUniquenessConstraintWithBtreeIndexType()
    {
        String name;
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withIndexType( BTREE ).create();
            name = constraint.getName();
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( name );
            assertThat( index.getIndexType(), is( BTREE ) );
        }
    }

    @Test
    void creatingUniquenessConstraintWithFullTextIndexTypeMustThrow()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withIndexType( FULLTEXT );
            assertThrows( IllegalArgumentException.class, creator::create );
        }
    }

    @Test
    void creatingNodePropertyExistenceConstraintMustThrowWhenGivenIndexType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).assertPropertyExists( propertyKey ).withIndexType( BTREE );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    @Test
    void creatingRelationshipPropertyExistenceConstraintsMustThrowWhenGivenIndexType()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( relType ).assertPropertyExists( propertyKey ).withIndexType( BTREE );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    @Test
    void mustBeAbleToSpecifyIndexConfigurationForUniquenessConstraint()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint = tx.schema().constraintFor( label ).withName( "my constraint" ).assertPropertyIsUnique( propertyKey )
                    .withIndexConfiguration( Map.of(
                            IndexSettingImpl.SPATIAL_CARTESIAN_MAX, new double[]{200.0, 200.0},
                            IndexSettingImpl.SPATIAL_WGS84_MIN, new double[]{-90.0, -90.0} ) )
                    .create();
            IndexDefinition index = tx.schema().getIndexByName( constraint.getName() );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertArrayEquals( new double[] {200.0, 200.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_CARTESIAN_MAX ) );
            assertArrayEquals( new double[] {-90.0, -90.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_WGS84_MIN ) );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            IndexDefinition index = tx.schema().getIndexByName( "my constraint" );
            Map<IndexSetting,Object> config = index.getIndexConfiguration();
            assertArrayEquals( new double[] {200.0, 200.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_CARTESIAN_MAX ) );
            assertArrayEquals( new double[] {-90.0, -90.0}, (double[]) config.get( IndexSettingImpl.SPATIAL_WGS84_MIN ) );
            tx.commit();
        }
    }

    @Test
    void creatingNodePropertyExistenceConstraintMustThrowWhenGivenIndexConfiguration()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( label ).withIndexConfiguration( Map.of() ).assertPropertyExists( propertyKey );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    @Test
    void creatingRelationshipPropertyExistenceConstraintMustThrowWhenGivenIndexConfiguration()
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintCreator creator = tx.schema().constraintFor( relType ).withIndexConfiguration( Map.of() ).assertPropertyExists( propertyKey );
            assertThrows( IllegalArgumentException.class, creator::create );
            tx.commit();
        }
    }

    private static String alreadyExistsIndexMessage( String indexName )
    {
        return "There already exists an index called '" + indexName + "'";
    }

    private static String thereAlreadyExistsConstraintMessage( String constraintName )
    {
        return "There already exists a constraint called '" + constraintName + "'.";
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
                    tx -> tx.schema().indexFor( label ).on( propertyKey ).withName( indexName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( indexName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @RepeatedTest( 20 )
        void cannotCreateConstraintsWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String constraintName = "MyConstraint";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    tx -> tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( constraintName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    tx -> tx.schema().constraintFor( otherLabel ).assertPropertyIsUnique( secondPropertyKey ).withName( constraintName ) ) );

            raceTransactions( firstFuture, secondFuture );

            assertOneSuccessAndOneFailure( firstFuture, secondFuture );
        }

        @RepeatedTest( 20 )
        void cannotCreateIndexesAndConstraintsWithTheSameNameInConcurrentTransactions() throws Exception
        {
            String schemaName = "MySchema";

            Future<Void> firstFuture = first.submit( schemaTransaction(
                    tx -> tx.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).withName( schemaName ) ) );
            Future<Void> secondFuture = second.submit( schemaTransaction(
                    tx -> tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( schemaName ) ) );

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
                tx.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
                tx.commit();
            }

            BinaryLatch afterFirstDropsConstraint = new BinaryLatch();
            BinaryLatch pauseFirst = new BinaryLatch();
            BinaryLatch beforeSecondCreatesIndex = new BinaryLatch();

            Future<Void> firstFuture = first.submit( () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().getConstraintByName( schemaName ).drop();
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
                    IndexCreator indexCreator = tx.schema().indexFor( otherLabel ).on( secondPropertyKey ).withName( schemaName );
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
                assertFalse( tx.schema().getConstraints().iterator().hasNext() );
                Iterator<IndexDefinition> indexes = tx.schema().getIndexes().iterator();
                assertTrue( indexes.hasNext() );
                assertEquals( indexes.next().getName(), schemaName );
                assertFalse( indexes.hasNext() );
                tx.commit();
            }
        }

        @RepeatedTest( 10 )
        void awaitIndexesMustNotThrowOnConcurrentlyDroppedIndexes() throws Exception
        {
            AtomicBoolean stop = new AtomicBoolean();
            Queue<IndexDefinition> indexes = new ConcurrentLinkedQueue<>();

            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < 50; i++ )
                {
                    indexes.add( tx.schema().indexFor( Label.label( "Label_" + i ) ).on( "propl_" + i ).create() );
                }
                tx.commit();
            }

            Future<Void> firstFuture = first.submit( () ->
            {
                startLatch.await();
                while ( !stop.get() )
                {
                    try ( Transaction tx = db.beginTx() )
                    {
                        try
                        {
                            tx.schema().awaitIndexesOnline( 20, TimeUnit.MINUTES );
                        }
                        catch ( Exception e )
                        {
                            stop.set( true );
                            indexes.clear();
                            throw e;
                        }
                        tx.commit();
                    }
                }
            } );

            Future<Void> secondFuture = second.submit( () ->
            {
                startLatch.await();
                IndexDefinition index;
                try
                {
                    while ( (index = indexes.poll()) != null )
                    {
                        try ( Transaction tx = db.beginTx() )
                        {
                            Thread.sleep( 1 );
                            tx.schema().getIndexByName( index.getName() ).drop();
                            tx.commit();
                        }
                    }
                }
                finally
                {
                    stop.set( true );
                }
                return null;
            } );

            raceTransactions( firstFuture, secondFuture );

            // Then these must not throw.
            firstFuture.get();
            secondFuture.get();
        }

        private Callable<Void> schemaTransaction( ThrowingFunction<Transaction, Object, Exception> action )
        {
            return () ->
            {
                try ( Transaction tx = db.beginTx() )
                {
                    Object creator = action.apply(tx);
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
            tx.schema().getConstraintByName( constraint.getName() ).drop();
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
            ConstraintCreator creator = tx.schema().constraintFor( label );
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
            tx.schema().getIndexByName( index.getName() ).drop();
            tx.commit();
        }
    }

    private Node createNode( GraphDatabaseService db, String key, Object value, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label );
            node.setProperty( key, value );
            tx.commit();
            return node;
        }
    }
}
