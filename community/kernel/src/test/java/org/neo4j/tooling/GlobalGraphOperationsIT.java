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
package org.neo4j.tooling;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;

import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.helpers.collection.Iterables.toSet;

public class GlobalGraphOperationsIT
{

    @Rule public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldListAllPropertyKeys() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "myProperty", 12);
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When
        try( Transaction _ = db.beginTx() )
        {
            assertThat( toList( gg.getAllPropertyKeys() ), equalTo( asList( "myProperty" ) ) );
        }
    }

    @Test
    public void shouldReturnAllLabels()
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Label dead = DynamicLabel.label( "dead" );
        Label alive = DynamicLabel.label( "alive" );
        try( Transaction tx = db.beginTx() )
        {
            db.createNode( alive );
            db.createNode( dead );
            tx.success();
        }

        try( Transaction tx = db.beginTx() )
        {
            Node node = IteratorUtil.single( db.findNodes( dead ) );
            node.delete();
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When - Then
        try( Transaction ignored = db.beginTx() )
        {
            assertThat( toSet( gg.getAllLabels() ), equalTo( toSet( asList( alive, dead ) ) ) );
        }
    }

    @Test
    public void shouldReturnAllInUseLabels()
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        Label dead = DynamicLabel.label( "dead" );
        Label alive = DynamicLabel.label( "alive" );
        try( Transaction tx = db.beginTx() )
        {
            db.createNode( alive );
            db.createNode( dead );
            tx.success();
        }

        try( Transaction tx = db.beginTx() )
        {
            Node node = IteratorUtil.single( db.findNodes( dead ) );
            node.delete();
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When - Then
        try( Transaction ignored = db.beginTx() )
        {
            assertThat( toSet( gg.getAllLabelsInUse() ), equalTo( Collections.singleton( alive ) ) );
        }
    }

    @Test
    public void shouldReturnAllRelationshipTypes()
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        RelationshipType dead = DynamicRelationshipType.withName( "DEAD" );
        RelationshipType alive = DynamicRelationshipType.withName( "ALIVE" );
        long deadId;
        try( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), alive );
            deadId = db.createNode().createRelationshipTo( db.createNode(), dead ).getId();
            tx.success();
        }

        try( Transaction tx = db.beginTx() )
        {
            db.getRelationshipById( deadId ).delete();
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When - Then
        try( Transaction ignored = db.beginTx() )
        {
            Iterable<String> result = map( new Function<RelationshipType,String>()
            {
                @Override
                public String apply( RelationshipType relationshipType )
                {
                    return relationshipType.name();
                }
            }, gg.getAllRelationshipTypes() );
            assertThat( toSet( result ), equalTo( toSet( asList( alive.name(), dead.name() ) ) ) );
        }
    }

    @Test
    public void shouldReturnAllInUseRelationshipTypes()
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        RelationshipType dead = DynamicRelationshipType.withName( "DEAD" );
        RelationshipType alive = DynamicRelationshipType.withName( "ALIVE" );
        long deadId;
        try( Transaction tx = db.beginTx() )
        {
            db.createNode().createRelationshipTo( db.createNode(), alive );
            deadId = db.createNode().createRelationshipTo( db.createNode(), dead ).getId();
            tx.success();
        }

        try( Transaction tx = db.beginTx() )
        {
            db.getRelationshipById( deadId ).delete();
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When - Then
        try( Transaction ignored = db.beginTx() )
        {
            Iterable<String> result = map( new Function<RelationshipType,String>()
            {
                @Override
                public String apply( RelationshipType relationshipType )
                {
                    return relationshipType.name();
                }
            }, gg.getAllRelationshipTypesInUse() );
            assertThat( toSet( result ) , equalTo( Collections.singleton( alive.name() ) ) );
        }
    }
}
