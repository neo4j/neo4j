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

import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.graphdb.UniqueEntities.byLegacyUniquenessFactory;
import static org.neo4j.graphdb.UniqueEntities.byUniquenessConstraint;

public class UniqueEntitiesTest
{
    @Rule
    public EmbeddedDatabaseRule dbRule = new EmbeddedDatabaseRule( UniqueEntitiesTest.class );

    private GraphDatabaseService graph;

    private Label label = DynamicLabel.label( "Person" );

    @Before
    public void before()
    {
        this.graph = dbRule.getGraphDatabaseService();
    }

    @Test
    public void shouldCreateNewNodesForUniqueConstraint() throws Exception
    {
        // given
        ConstraintDefinition constraint = createConstraint();

        try ( Transaction ignored = graph.beginTx() )
        {
            UniqueEntities.Creator<Node> nodeCreator = byUniquenessConstraint( graph, constraint );

            // when
            UniqueEntities.Result<Node> result = nodeCreator.getOrCreate( "Ben" );

            // then
            assertTrue( result.wasCreated() );
            assertEquals( "Ben", result.entity().getProperty( "name" ) );
        }
    }

    @Test
    public void shouldNotRecreateExistingNodeForUniqueConstraint() throws Exception
    {
        // given
        ConstraintDefinition constraint = createConstraint();

        try ( Transaction ignored = graph.beginTx() )
        {
            UniqueEntities.Creator<Node> nodeCreator = byUniquenessConstraint( graph, constraint );

            // when
            UniqueEntities.Result<Node> result1 = nodeCreator.getOrCreate( "Ben" );
            UniqueEntities.Result<Node> result2 = nodeCreator.getOrCreate( "Ben" );

            // then
            assertFalse( result2.wasCreated() );
            assertEquals( result1.entity().getId(), result2.entity().getId() );
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldCreateNewNodesForUniqueNodeFactory() throws Exception
    {
        // given
        Index<Node> index = mock( Index.class );
        when( index.getGraphDatabase() ).thenReturn( graph );
        IndexHits indexHits = mock( IndexHits.class );
        when( indexHits.getSingle() ).thenReturn( null );
        when( index.get( "name", "Ben" ) ).thenReturn( indexHits );
        when( index.putIfAbsent( any( Node.class ), anyString(), any() ) ).thenReturn( null );

        UniqueFactory.UniqueNodeFactory nodeFactory = createUniqueNodeFactory( index );

        try ( Transaction ignored = graph.beginTx() )
        {
            UniqueEntities.Creator<Node> nodeCreator = byLegacyUniquenessFactory( nodeFactory, "name" );

            // when
            UniqueEntities.Result<Node> result = nodeCreator.getOrCreate( "Ben" );

            // then
            assertTrue( result.wasCreated() );
            assertEquals( "Ben", result.entity().getProperty( "name" ) );
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotRecreateExistingNodeForUniqueNodeFactory() throws Exception
    {
        // given
        Index<Node> index = mock( Index.class );
        when( index.getGraphDatabase() ).thenReturn( graph );
        IndexHits indexHits = mock( IndexHits.class );
        Node nodeMock = mock( Node.class );
        when( indexHits.getSingle() ).thenReturn( nodeMock );
        when( index.get( "name", "Ben" ) ).thenReturn( indexHits );

        UniqueFactory.UniqueNodeFactory nodeFactory = createUniqueNodeFactory( index );

        try ( Transaction ignored = graph.beginTx() )
        {
            UniqueEntities.Creator<Node> nodeCreator = byLegacyUniquenessFactory( nodeFactory, "name" );

            // when
            UniqueEntities.Result<Node> result = nodeCreator.getOrCreate( "Ben" );

            // then
            assertFalse( result.wasCreated() );
            assertEquals( nodeMock, result.entity() );
        }
    }

    private UniqueFactory.UniqueNodeFactory createUniqueNodeFactory( Index<Node> index )
    {
        try ( Transaction tx = graph.beginTx() )
        {
            UniqueFactory.UniqueNodeFactory nodeFactory = new UniqueFactory.UniqueNodeFactory( index )
            {
                @Override
                protected void initialize( Node created, Map<String, Object> properties )
                {
                    for ( Map.Entry<String, Object> entry : properties.entrySet() )
                    {
                        created.setProperty( entry.getKey(), entry.getValue() );
                    }
                }
            };
            tx.success();
            return nodeFactory;
        }
    }

    private ConstraintDefinition createConstraint()
    {
        try ( Transaction tx = graph.beginTx() )
        {
            ConstraintDefinition constraint =
                    graph.schema().constraintFor( label ).assertPropertyIsUnique( "name" ).create();
            tx.success();
            return constraint;
        }
    }
}
