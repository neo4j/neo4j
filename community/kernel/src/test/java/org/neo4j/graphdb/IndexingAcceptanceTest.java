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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.Iterables.single;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.ImpermanentDatabaseRule;

public class IndexingAcceptanceTest
{
    public
    @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label
    {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    @Test
    public void searchingForNodeByPropertyShouldWorkWithoutIndex() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel( Labels.MY_LABEL );
            myNode.setProperty( "name", "Hawking" );

            tx.success();
        } finally
        {
            tx.finish();
        }

        // Then
        Node result = single( beansAPI.findByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );
        assertThat( result, is( myNode ) );
    }

    @Test @Ignore("TODO: Un-ignore once we can read from Lucene")
    public void searchingUsesIndexWhenItExists() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode;

        // When
        Transaction tx = beansAPI.beginTx();
        IndexDefinition indexDef;
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel( Labels.MY_LABEL );
            myNode.setProperty( "name", "Hawking" );
            indexDef = beansAPI.schema().indexCreator( Labels.MY_LABEL ).on( "name" ).create();

            tx.success();
        } finally
        {
            tx.finish();
        }

        while ( beansAPI.schema().getIndexState( indexDef ) == Schema.IndexState.POPULATING )
        {
            Thread.sleep( 10 );
        }

        assertThat( beansAPI.schema().getIndexState( indexDef ), is( Schema.IndexState.ONLINE ) );


        // Then
        Node result = single( beansAPI.findByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" ) );
        assertThat( result, is( myNode ) );
    }

    @Test
    public void searchingByLabelAndPropertyReturnsEmptyWhenMissingLabelOrProperty() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();

        // When/Then
        Iterable<Node> result = beansAPI.findByLabelAndProperty( Labels.MY_LABEL, "name", "Hawking" );
        assertThat( result, is( Iterables.<Node>empty() ) );
    }
}
