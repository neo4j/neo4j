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
package org.neo4j.server.rest.domain;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.traversal.BranchState.NO_STATE;
import static org.neo4j.graphdb.traversal.Paths.singleNodePath;
import static org.neo4j.helpers.collection.Iterables.asSet;
import static org.neo4j.helpers.collection.MapUtil.map;

public class RelationshipExpanderBuilderTest
{
    @ClassRule
    public static final DatabaseRule db = new ImpermanentDatabaseRule();

    @Test
    public void shouldInterpretNoSpecifiedRelationshipsAsAll()
    {
        // GIVEN
        Node node = createSomeData();
        PathExpander expander = RelationshipExpanderBuilder.describeRelationships( map() );

        // WHEN
        Set<Relationship> expanded;
        try ( Transaction tx = db.beginTx() )
        {
            expanded = asSet( expander.expand( singleNodePath( node ), NO_STATE ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // THEN
            assertEquals( asSet( node.getRelationships() ), expanded );
            tx.success();
        }
    }

    @Test
    public void shouldInterpretSomeSpecifiedRelationships()
    {
        // GIVEN
        Node node = createSomeData();
        PathExpander expander = RelationshipExpanderBuilder.describeRelationships(
                map( "relationships", map(
                        "type", MyRelTypes.TEST.name(),
                        "direction", RelationshipDirection.out.name() ) ) );

        // WHEN
        Set<Relationship> expanded;
        try ( Transaction tx = db.beginTx() )
        {
            expanded = asSet( expander.expand( singleNodePath( node ), NO_STATE ) );
            tx.success();
        }

        try ( Transaction tx = db.beginTx() )
        {
            // THEN
            assertEquals( asSet( node.getRelationships( MyRelTypes.TEST ) ), expanded );
            tx.success();
        }
    }

    private Node createSomeData()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            node.createRelationshipTo( db.createNode(), MyRelTypes.TEST2 );
            tx.success();
            return node;
        }
    }
}
