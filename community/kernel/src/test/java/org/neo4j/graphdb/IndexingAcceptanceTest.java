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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.test.ImpermanentDatabaseRule;

public class IndexingAcceptanceTest
{
    @Test
    public void searchingForNodeByPropertyShouldWorkWithoutIndex() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), Labels.MY_LABEL );

        // When
        Node result = single( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );

        // Then
        assertEquals( result, myNode );
    }

    @Test
    public void searchingUsesIndexWhenItExists() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = createNode( beansAPI, map( "name", "Hawking" ), Labels.MY_LABEL );
        createIndex( beansAPI, Labels.MY_LABEL, "name" );

        // When
        Node result = single( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );

        // Then
        assertEquals( result, myNode );
    }

    @Test
    public void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When/Then
        Iterable<Node> result = beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" );
        assertEquals( asSet(), asSet( result ) );
    }
    
    @Test
    public void shouldSeeIndexUpdatesWhenQueryingOutsideTransaction() throws Exception
    {
        // GIVEN
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        createIndex( beansAPI, Labels.MY_LABEL, "name" );
        Node firstNode = createNode( beansAPI, map( "name", "Mattias" ), Labels.MY_LABEL );

        // WHEN
        Set<Node> firstResult = asSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Mattias" ) );
        Node secondNode = createNode( beansAPI, map( "name", "Taylor" ), Labels.MY_LABEL );
        Set<Node> secondResult = asSet( beansAPI.findNodesByLabelAndProperty( Labels.MY_LABEL, "name", "Taylor" ) );

        // THEN
        assertEquals( asSet( firstNode ), firstResult );
        assertEquals( asSet( secondNode ), secondResult );
    }

    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL, MY_OTHER_LABEL
    }

    private Node createNode( GraphDatabaseService beansAPI, Map<String, Object> properties, Label... labels )
    {
        Transaction tx = beansAPI.beginTx();
        try
        {
            Node node = beansAPI.createNode( labels );
            for ( Map.Entry<String,Object> property : properties.entrySet() )
                node.setProperty( property.getKey(), property.getValue() );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    private IndexDefinition createIndex( GraphDatabaseService beansAPI, Label label, String property )
    {
        Transaction tx = beansAPI.beginTx();
        IndexDefinition indexDef;
        try
        {
            indexDef = beansAPI.schema().indexCreator( label ).on( property ).create();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        beansAPI.schema().awaitIndexOnline( indexDef, 10, SECONDS );
        return indexDef;
    }
}
