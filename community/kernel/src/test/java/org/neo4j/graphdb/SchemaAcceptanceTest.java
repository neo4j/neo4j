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

import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.ConstraintType;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.schema.Schema.IndexState;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.helpers.Function;
import org.neo4j.test.ImpermanentDatabaseRule;

public class SchemaAcceptanceTest
{
    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    @Test
    public void addingAnIndexingRuleShouldSucceed() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Schema schema = beansAPI.schema();
        String property = "my_property_key";
        Labels label = Labels.MY_LABEL;

        // When
        IndexDefinition index = createIndexRule( beansAPI, label, property );

        // Then
        assertEquals( asSet( property ), asSet( singlePropertyKey( schema.getIndexes( label ) ) ) );
        assertTrue( asSet( schema.getIndexes( label ) ).contains( index ) );
        
        // Then
        Iterable<IndexDefinition> indexes = schema.getIndexes( Labels.MY_LABEL );

        assertEquals( asSet( property ), asSet( singlePropertyKey( indexes ) ) );
        schema.awaitIndexOnline( single( indexes), 5L, TimeUnit.SECONDS );
    }

    @Test
    public void shouldThrowConstraintViolationIfAskedToIndexSamePropertyAndLabelTwiceInSameTx() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Schema schema = beansAPI.schema();
        String property = "my_property_key";

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
            try
            {
                schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
                fail( "Should not have validated" );
            }
            catch ( ConstraintViolationException e )
            {
                assertThat( e.getMessage(), containsString( "Unable to create index" ) );
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
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Schema schema = beansAPI.schema();
        String property = "my_property_key";

        // And given
        Transaction tx = beansAPI.beginTx();
        schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
        tx.success();
        tx.finish();

        // When
        ConstraintViolationException caught = null;
        tx = beansAPI.beginTx();
        try
        {
            schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
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
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Schema schema = beansAPI.schema();

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            schema.indexCreator( Labels.MY_LABEL )
                    .on( "my_property_key" )
                    .on( "other_property" ).create();
            tx.success();
            fail( "Should not be able to create index on multiple property keys" );
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
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Labels label = Labels.MY_LABEL;
        IndexDefinition index = createIndexRule( beansAPI, label, property );

        // WHEN
        dropIndex( beansAPI, index );

        // THEN
        assertFalse( "Index should have been deleted", asSet( beansAPI.schema().getIndexes( label ) ).contains( index ) );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSameTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Labels label = Labels.MY_LABEL;
        IndexDefinition index = createIndexRule( beansAPI, label, property );

        // WHEN
        Transaction tx = beansAPI.beginTx();
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
        assertFalse( "Index should have been deleted", asSet( beansAPI.schema().getIndexes( label ) ).contains( index ) );
    }

    @Test
    public void droppingAnUnexistingIndexShouldGiveHelpfulExceptionInSeparateTransactions() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Labels label = Labels.MY_LABEL;
        IndexDefinition index = createIndexRule( beansAPI, label, property );
        dropIndex( beansAPI, index );

        // WHEN
        try
        {
            dropIndex( beansAPI, index );
            fail( "Should not be able to drop index twice" );
        }
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getMessage(), containsString( "Unable to drop index" ) );
        }

        // THEN
        assertFalse( "Index should have been deleted", asSet( beansAPI.schema().getIndexes( label ) ).contains( index ) );
    }

    @Test
    public void awaitingIndexComingOnlineWorks()
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Labels label = Labels.MY_LABEL;

        // WHEN
        IndexDefinition index = createIndexRule( beansAPI, label, property );

        // PASS
        beansAPI.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

        // THEN
        assertEquals( Schema.IndexState.ONLINE, beansAPI.schema().getIndexState( index ) );
    }
    
    @Test
    public void awaitingAllIndexesComingOnlineWorks()
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Labels label = Labels.MY_LABEL;

        // WHEN
        IndexDefinition index = createIndexRule( beansAPI, label, property );

        // PASS
        beansAPI.schema().awaitIndexesOnline( 1L, TimeUnit.MINUTES );

        // THEN
        assertEquals( Schema.IndexState.ONLINE, beansAPI.schema().getIndexState( index ) );
    }

    @Test
    public void shouldRecreateDroppedIndex() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        String property = "name";
        Label label = Labels.MY_LABEL;
        Node node = createNode( beansAPI, property, "Neo", label );
        
        // create an index
        IndexDefinition index = createIndexRule( beansAPI, label, property );
        beansAPI.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );
        
        // delete the index right away
        dropIndex( beansAPI, index );

        // WHEN recreating that index
        createIndexRule( beansAPI, label, property );
        beansAPI.schema().awaitIndexOnline( index, 1L, TimeUnit.MINUTES );

        // THEN it should exist and be usable
        index = single( beansAPI.schema().getIndexes( label ) );
        assertEquals( IndexState.ONLINE, beansAPI.schema().getIndexState( index ) );
        assertEquals( asSet( node ), asSet( beansAPI.findNodesByLabelAndProperty( label, property, "Neo" ) ) );
    }
    
    @Test
    public void shouldCreateUniquenessConstraint() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        String propertyKey = "name";

        // WHEN
        ConstraintDefinition constraint =
                createConstraint( db, db.schema().constraintCreator( label ).on( propertyKey ).unique() );

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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        String propertyKey = "name";
        ConstraintDefinition createdConstraint = createConstraint( db,
                db.schema().constraintCreator( label ).on( propertyKey ).unique() );

        // WHEN
        Iterable<ConstraintDefinition> listedConstraints = db.schema().getConstraints( label );

        // THEN
        assertEquals( createdConstraint, single( listedConstraints ) );
    }

    @Test
    public void shouldListAddedConstraints() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        String propertyKey = "name";
        ConstraintDefinition createdConstraint =
                createConstraint( db, db.schema().constraintCreator( label ).on( propertyKey ).unique() );

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
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        Label label = Labels.MY_LABEL;
        String propertyKey = "name";
        ConstraintDefinition constraint =
                createConstraint( db, db.schema().constraintCreator( label ).on( propertyKey ).unique() );
        
        // WHEN
        dropConstraint( db, constraint );
        
        // THEN
        assertEquals( 0, count( db.schema().getConstraints( label ) ) );
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

    private ConstraintDefinition createConstraint( GraphDatabaseService db, ConstraintCreator constraintCreator )
    {
        Transaction tx = db.beginTx();
        try
        {
            ConstraintDefinition constraint = constraintCreator.create();
            tx.success();
            return constraint;
        }
        finally
        {
            tx.finish();
        }
    }

    private IndexDefinition createIndexRule( GraphDatabaseService beansAPI, Label label, String property )
    {
        Transaction tx = beansAPI.beginTx();
        try
        {
            IndexDefinition result = beansAPI.schema().indexCreator( label ).on( property ).create();
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    private void dropIndex( GraphDatabaseService beansAPI, IndexDefinition index )
    {
        Transaction tx = beansAPI.beginTx();
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
