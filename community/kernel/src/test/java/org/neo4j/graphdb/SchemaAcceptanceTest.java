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
package org.neo4j.graphdb;

import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.helpers.Function;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.IteratorUtil.single;

public class SchemaAcceptanceTest
{
    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
    private GraphDatabaseService db;
    private Label label = Labels.MY_LABEL;
    private String propertyKey = "my_property_key";


    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    @Test
    public void addingAnIndexingRuleShouldSucceed() throws Exception
    {
        // Given
        Schema schema = db.schema();

        // When
        IndexDefinition index = createIndex( label, propertyKey );

        // Then
        assertEquals( asSet( propertyKey ), asSet( singlePropertyKey( schema.getIndexes( label ) ) ) );
        assertTrue( asSet( schema.getIndexes( label ) ).contains( index ) );
        
        // Then
        Iterable<IndexDefinition> indexes = schema.getIndexes( Labels.MY_LABEL );

        assertEquals( asSet( propertyKey ), asSet( singlePropertyKey( indexes ) ) );
        schema.awaitIndexOnline( single( indexes), 5L, TimeUnit.SECONDS );
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToIndexSamePropertyAndLabelTwiceInSameTx() throws Exception
    {
        // Given
        Schema schema = db.schema();

        // When
        Transaction tx = db.beginTx();
        try
        {
            schema.indexFor( label ).on( propertyKey ).create();
            try
            {
                schema.indexFor( label ).on( propertyKey ).create();
                fail( "Should not have validated" );
            }
            catch ( ConstraintViolationException e )
            {
                assertEquals( "Unable to add index on [label: MY_LABEL, my_property_key] : Already " +
                        "indexed :MY_LABEL(my_property_key).", e.getMessage() );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToIndexPropertyThatIsAlreadyIndexed() throws Exception
    {
        // Given
        Schema schema = db.schema();

        // And given
        Transaction tx = db.beginTx();
        schema.indexFor( label ).on( propertyKey ).create();
        tx.success();
        tx.finish();

        // When
        ConstraintViolationException caught = null;
        tx = db.beginTx();
        try
        {
            schema.indexFor( label ).on( propertyKey ).create();
            tx.success();
        }
        catch(ConstraintViolationException e)
        {
            caught = e;
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertThat(caught, not(nullValue()));
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToCreateCompoundIdex() throws Exception
    {
        // Given
        Schema schema = db.schema();

        // When
        Transaction tx = db.beginTx();
        try
        {
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
        finally
        {
            tx.finish();
        }
    }
    
    @Test
    public void droppingExistingIndexRuleShouldSucceed() throws Exception
    {
        // GIVEN
        IndexDefinition index = createIndex( label, propertyKey );

        // WHEN
        dropIndex( index );

        // THEN
        assertFalse( "Index should have been deleted", asSet( db.schema().getIndexes( label ) ).contains( index
        ) );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSameTransaction() throws Exception
    {
        // GIVEN
        IndexDefinition index = createIndex( label, propertyKey );

        // WHEN
        Transaction tx = db.beginTx();
        try
        {
            index.drop();
            try
            {
                index.drop();
                fail( "Should not be able to drop index twice" );
            }
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getMessage(), containsString( "Unable to drop index" ) );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // THEN
        assertFalse( "Index should have been deleted", asSet( db.schema().getIndexes( label ) ).contains( index ) );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSeparateTransactions() throws Exception
    {
        // GIVEN
        IndexDefinition index = createIndex( label, propertyKey );
        dropIndex( index );

        // WHEN
        try
        {
            dropIndex( index );
            fail( "Should not be able to drop index twice" );
        }
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "Unable to drop index" ) );
        }

        // THEN
        assertFalse( "Index should have been deleted", asSet( db.schema().getIndexes( label ) ).contains( index ) );
    }

    @Test
    public void awaitingIndexComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( label, propertyKey );

        // PASS
        db.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

        // THEN
        assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
    }
    
    @Test
    public void awaitingAllIndexesComingOnlineWorks()
    {
        // GIVEN

        // WHEN
        IndexDefinition index = createIndex( label, propertyKey );
        createIndex( label, "other_property" );

        // PASS
        db.schema().awaitIndexesOnline( 1L, TimeUnit.MINUTES );

        // THEN
        assertEquals( Schema.IndexState.ONLINE, db.schema().getIndexState( index ) );
    }

    @Test
    public void shouldRecreateDroppedIndex() throws Exception
    {
        // GIVEN
        Node node = createNode( db, propertyKey, "Neo", label );

        // create an index
        IndexDefinition index = createIndex( label, propertyKey );
        db.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

        // delete the index right away
        dropIndex( index );

        // WHEN recreating that index
        createIndex( label, propertyKey );
        db.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

        // THEN it should exist and be usable
        index = single( db.schema().getIndexes( label ) );
        assertEquals( IndexState.ONLINE, db.schema().getIndexState( index ) );
        assertEquals( asSet( node ), asSet( db.findNodesByLabelAndProperty( label, propertyKey, "Neo" ) ) );
    }
    
    @Test
    public void shouldCreateUniquenessConstraint() throws Exception
    {
        // GIVEN

        // WHEN
        ConstraintDefinition constraint = createConstraint( label, propertyKey );

        // THEN
        assertEquals( ConstraintType.UNIQUENESS, constraint.getConstraintType() );
        
        UniquenessConstraintDefinition uniquenessConstraint = constraint.asUniquenessConstraint();
        assertEquals( label.name(), uniquenessConstraint.getLabel().name() );
        assertEquals( asSet( propertyKey ), asSet( uniquenessConstraint.getPropertyKeys() ) );
    }
    
    @Test
    public void shouldListAddedConstraintsByLabel() throws Exception
    {
        // GIVEN
        ConstraintDefinition createdConstraint = createConstraint( label, propertyKey );

        // WHEN
        Iterable<ConstraintDefinition> listedConstraints = db.schema().getConstraints( label );

        // THEN
        assertEquals( createdConstraint, single( listedConstraints ) );
    }

    @Test
    public void shouldListAddedConstraints() throws Exception
    {
        // GIVEN
        ConstraintDefinition createdConstraint = createConstraint( label, propertyKey );

        // WHEN
        Iterable<ConstraintDefinition> listedConstraints = db.schema().getConstraints();

        // THEN
        ConstraintDefinition foundConstraint = single( listedConstraints );
        assertEquals( createdConstraint, foundConstraint );
    }

    @Test
    public void shouldDropUniquenessConstraint() throws Exception
    {
        // GIVEN
        ConstraintDefinition constraint = createConstraint( label, propertyKey );

        // WHEN
        dropConstraint( db, constraint );
        
        // THEN
        assertEquals( 0, count( db.schema().getConstraints( label ) ) );
    }

    @Test
    public void addingConstraintWhenIndexAlreadyExistsGivesNiceError() throws Exception
    {
        // GIVEN
        createIndex( label, propertyKey );

        // WHEN
        try
        {
            createConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals(
                String.format("Unable to create CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key " +
                    "IS UNIQUE:%nUnable to add index on [label: MY_LABEL, my_property_key] : " +
                    "Already indexed :MY_LABEL(my_property_key)."), e.getMessage() );
        }
    }

    @Test
    public void addingUniquenessConstraintWhenDuplicateDataExistsGivesNiceError() throws Exception
    {
        // GIVEN
        Transaction transaction = db.beginTx();
        try {
            db.createNode( label ).setProperty( propertyKey, "value1" );
            db.createNode( label ).setProperty( propertyKey, "value1" );
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }

        // WHEN
        try
        {
            createConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals(
                String.format( "Unable to create CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key " +
                        "IS UNIQUE:%nMultiple nodes with label `MY_LABEL` have property `my_property_key` = " +
                        "'value1':%n" +
                        "  existing node(1)%n" +
                        "  new node(2)" ), e.getMessage() );
        }
    }

    @Test
    public void addingConstraintWhenAlreadyConstrainedGivesNiceError() throws Exception
    {
        // GIVEN
        createConstraint( label, propertyKey );

        // WHEN
        try
        {
            createConstraint( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "Already constrained CONSTRAINT ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key IS" +
                    " UNIQUE.", e.getMessage() );
        }
    }

    @Test
    public void addingIndexWhenAlreadyConstrained() throws Exception
    {
        // GIVEN
        createConstraint( label, propertyKey );

        // WHEN
        try
        {
            createIndex( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "Unable to add index on [label: MY_LABEL, my_property_key] : Already constrained CONSTRAINT" +
                    " ON ( my_label:MY_LABEL ) ASSERT my_label.my_property_key IS UNIQUE.", e.getMessage() );
        }
    }

    @Test
    public void addingIndexWhenAlreadyIndexed() throws Exception
    {
        // GIVEN
        createIndex( label, propertyKey );

        // WHEN
        try
        {
            createIndex( label, propertyKey );
            fail( "Expected exception to be thrown" );
        }
        catch ( ConstraintViolationException e )
        {
            assertEquals( "Unable to add index on [label: MY_LABEL, my_property_key] : Already indexed " +
                    ":MY_LABEL(my_property_key).", e.getMessage() );
        }
    }

    @Before
    public void init()
    {
        db = dbRule.getGraphDatabaseService();
    }

    private void dropConstraint( GraphDatabaseService db, ConstraintDefinition constraint )
    {
        Transaction tx = db.beginTx();
        try
        {
            constraint.drop();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private ConstraintDefinition createConstraint( Label label, String prop )
    {
        Transaction tx = db.beginTx();
        try
        {
            ConstraintDefinition constraint = db.schema().constraintFor( label ).on( prop ).unique().create();
            tx.success();
            return constraint;
        }
        finally
        {
            tx.finish();
        }
    }

    private IndexDefinition createIndex( Label label, String property )
    {
        Transaction tx = db.beginTx();
        try
        {
            IndexDefinition result = db.schema().indexFor( label ).on( property ).create();
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    private void dropIndex( IndexDefinition index )
    {
        Transaction tx = db.beginTx();
        try
        {
            index.drop();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Iterable<String> singlePropertyKey( Iterable<IndexDefinition> indexes )
    {
        return map( new Function<IndexDefinition, String>()
        {
            @Override
            public String apply( IndexDefinition from )
            {
                return single( from.getPropertyKeys() );
            }
        }, indexes );
    }
    
    private Node createNode( GraphDatabaseService db, String key, Object value, Label label )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode( label );
            node.setProperty( key, value );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
}
