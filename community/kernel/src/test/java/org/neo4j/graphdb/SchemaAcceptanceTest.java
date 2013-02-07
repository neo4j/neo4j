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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.single;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
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

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // Then
        assertEquals( asSet( property ), asSet( singlePropertyKey( schema.getIndexes( Labels.MY_LABEL ) ) ) );
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

    @Test(expected = ConstraintViolationException.class)
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
            schema.indexCreator( Labels.MY_LABEL ).on( property ).create();
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

    @Test(expected = UnsupportedOperationException.class)
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
        }
        finally
        {
            tx.finish();
        }
    }
}
