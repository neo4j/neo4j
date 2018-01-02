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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Neo4jMatchers.contains;
import static org.neo4j.graphdb.Neo4jMatchers.containsOnly;
import static org.neo4j.graphdb.Neo4jMatchers.createIndex;
import static org.neo4j.graphdb.Neo4jMatchers.findNodesByLabelAndProperty;
import static org.neo4j.graphdb.Neo4jMatchers.getConstraints;
import static org.neo4j.graphdb.Neo4jMatchers.getIndexes;
import static org.neo4j.graphdb.Neo4jMatchers.isEmpty;
import static org.neo4j.graphdb.Neo4jMatchers.waitForIndex;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class SchemaAcceptanceTest
{
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private GraphDatabaseService db;
    private Label label = Labels.MY_LABEL;
    private String propertyKey = "my_property_key";

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    private enum Types implements RelationshipType
    {
        MY_TYPE,
        MY_OTHER_TYPE
    }

    @Test
    public void addingAnIndexingRuleShouldSucceed() throws Exception
    {
        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // THEN
        assertThat( getIndexes( db, label ), containsOnly( index ) );
    }

    @Test
    public void addingAnIndexingRuleInNestedTxShouldSucceed() throws Exception
    {
        IndexDefinition index;

        // WHEN
        IndexDefinition indexDef;
        try ( Transaction tx = db.beginTx() )
        {
            try ( Transaction nestedTransaction = db.beginTx() )
            {
                indexDef = db.schema().indexFor( label ).on( propertyKey ).create();
                nestedTransaction.success();
            }

            index = indexDef;
            tx.success();
        }
        waitForIndex( db, indexDef );

        // THEN
        assertThat( getIndexes( db, label ), containsOnly( index ) );
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToIndexSamePropertyAndLabelTwiceInSameTx() throws Exception
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
                assertEquals( "There already exists an index for label 'MY_LABEL' on property 'my_property_key'.",
                        e.getMessage() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToIndexPropertyThatIsAlreadyIndexed() throws Exception
    {
        // GIVEN
        Schema schema;
        try ( Transaction tx = db.beginTx() )
        {
            schema = db.schema();
            schema.indexFor( label ).on( propertyKey ).create();
            tx.success();
        }

        // WHEN
        ConstraintViolationException caught = null;
        try ( Transaction tx = db.beginTx() )
        {
            schema.indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
        catch ( ConstraintViolationException e )
        {
            caught = e;
        }

        // THEN
        assertThat( caught, not( nullValue() ) );
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToCreateCompoundIndex() throws Exception
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.indexFor( label )
                    .on( "my_property_key" )
                    .on( "other_property" ).create();
            tx.success();
            fail( "Should not be able to create index on multiple propertyKey keys" );
        }
        catch ( UnsupportedOperationException e )
        {
            assertThat( e.getMessage(), containsString( "Compound indexes" ) );
        }
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToCreateCompoundConstraint() throws Exception
    {
        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            Schema schema = db.schema();
            schema.constraintFor( label )
                    .assertPropertyIsUnique( "my_property_key" )
                    .assertPropertyIsUnique( "other_property" ).create();
            tx.success();
            fail( "Should not be able to create constraint on multiple propertyKey keys" );
        }
        catch ( UnsupportedOperationException e )
        {
            assertThat( e.getMessage(), containsString( "can only create one unique constraint" ) );
        }
    }

    @Test
    public void droppingExistingIndexRuleShouldSucceed() throws Exception
    {
        // GIVEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // WHEN
        dropIndex( index );

        // THEN
        assertThat( getIndexes( db, label ), isEmpty() );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSameTransaction() throws Exception
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
                assertThat( e.getMessage(), containsString( "Index for label 'MY_LABEL' and property " +
                                                            "'my_property_key' not found." ) );
            }
            tx.success();
        }

        // THEN
        assertThat( "Index should have been deleted", getIndexes( db, label ), not( contains( index ) ) );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSeparateTransactions() throws Exception
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
            assertThat( e.getMessage(), containsString( "Index for label 'MY_LABEL' and property " +
                                                        "'my_property_key' not found." ) );
        }

        // THEN
        assertThat( "Index should have been deleted", getIndexes( db, label ), not( contains( index ) ) );
    }

    @Test
    public void awaitingIndexComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );

        // PASS
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
        }
    }

    @Test
    public void awaitingAllIndexesComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( db, label, propertyKey );
        createIndex( db, label, "other_property" );

        // PASS
        waitForIndex( db, index );
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1L, TimeUnit.MINUTES );

            // THEN
            assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
        }
    }

    @Test
    public void shouldRecreateDroppedIndex() throws Exception
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

        // THEN it should exist and be usable
        assertThat( getIndexes( db, label ), contains( index ) );
        assertThat( findNodesByLabelAndProperty( label, propertyKey, "Neo", db ), containsOnly( node ) );
    }

    @Test
    public void shouldCreateUniquenessConstraint() throws Exception
    {
        // WHEN
        ConstraintDefinition constraint = createUniquenessConstraint( label, propertyKey );

        // THEN
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( ConstraintType.UNIQUENESS, constraint.getConstraintType() );

            assertEquals( label.name(), constraint.getLabel().name() );
            assertEquals( asSet( propertyKey ), asSet( constraint.getPropertyKeys() ) );
            tx.success();
        }
    }

    @Test
    public void shouldListAddedConstraintsByLabel() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( label, propertyKey );
        createUniquenessConstraint( Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        assertThat( getConstraints( db, label ), containsOnly( constraint1 ) );
    }

    @Test
    public void shouldListAddedConstraints() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint1 = createUniquenessConstraint( Labels.MY_LABEL, propertyKey );
        ConstraintDefinition constraint2 = createUniquenessConstraint( Labels.MY_OTHER_LABEL, propertyKey );

        // WHEN THEN
        assertThat( getConstraints( db ), containsOnly( constraint1, constraint2 ) );
    }

    @Test
    public void shouldDropUniquenessConstraint() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint = createUniquenessConstraint( label, propertyKey );

        // WHEN
        dropConstraint( db, constraint );

        // THEN
        assertThat( getConstraints( db, label ), isEmpty() );
    }

    @Test
    public void addingConstraintWhenIndexAlreadyExistsGivesNiceError() throws Exception
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
            assertEquals( "There already exists an index for label 'MY_LABEL' on property 'my_property_key'. " +
                          "A constraint cannot be created until the index has been dropped.", e.getMessage() );
        }
    }

    @Test
    public void addingUniquenessConstraintWhenDuplicateDataExistsGivesNiceError() throws Exception
    {
        // GIVEN
        try ( Transaction transaction = db.beginTx() )
        {
            db.createNode( label ).setProperty( propertyKey, "value1" );
            db.createNode( label ).setProperty( propertyKey, "value1" );
            transaction.success();
        }

        // WHEN
        try
        {
            createUniquenessConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals(
                    format( "Unable to create CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key " +
                            "IS UNIQUE:%nMultiple nodes with label `MY_LABEL` have property `my_property_key` = " +
                            "'value1':%n" +
                            "  node(0)%n" +
                            "  node(1)" ), e.getMessage() );
        }
    }

    @Test
    public void addingConstraintWhenAlreadyConstrainedGivesNiceError() throws Exception
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
                    "Constraint already exists: CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key " +
                    "IS UNIQUE",
                    e.getMessage() );
        }
    }

    @Test
    public void addingIndexWhenAlreadyConstrained() throws Exception
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
            assertEquals(
                    "Label 'MY_LABEL' and property 'my_property_key' have a unique constraint defined on them, so an " +
                    "index is " +
                    "already created that matches this.", e.getMessage() );
        }
    }

    @Test
    public void addingIndexWhenAlreadyIndexed() throws Exception
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
            assertEquals( "There already exists an index for label 'MY_LABEL' on property 'my_property_key'.",
                    e.getMessage() );
        }
    }

    @Test
    public void addedUncommittedIndexesShouldBeVisibleWithinTheTransaction()
    {
        // GIVEN
        IndexDefinition indexA = createIndex( db, label, "a" );
        createUniquenessConstraint( label, "b" );

        // WHEN
        try ( Transaction tx = db.beginTx() )
        {
            assertThat( count( db.schema().getIndexes( label ) ), is( 2L ) );
            IndexDefinition indexC = db.schema().indexFor( label ).on( "c" ).create();
            // THEN
            assertThat( count( db.schema().getIndexes( label ) ), is( 3L ) );
            assertThat( db.schema().getIndexState( indexA ), is( Schema.IndexState.ONLINE ) );
            assertThat( db.schema().getIndexState( indexC ), is( Schema.IndexState.POPULATING ) );
        }
    }

    @Before
    public void init()
    {
        db = dbRule.getGraphDatabaseService();
    }

    private void dropConstraint( GraphDatabaseService db, ConstraintDefinition constraint )
    {
        try ( Transaction tx = db.beginTx() )
        {
            constraint.drop();
            tx.success();
        }
    }

    private ConstraintDefinition createUniquenessConstraint( Label label, String prop )
    {
        try ( Transaction tx = db.beginTx() )
        {
            ConstraintDefinition constraint =
                    db.schema().constraintFor( label ).assertPropertyIsUnique( prop ).create();
            tx.success();
            return constraint;
        }
    }

    private void dropIndex( IndexDefinition index )
    {
        try ( Transaction tx = db.beginTx() )
        {
            index.drop();
            tx.success();
        }
    }

    private Node createNode( GraphDatabaseService db, String key, Object value, Label label )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label );
            node.setProperty( key, value );
            tx.success();
            return node;
        }
    }
}
