/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.scenarios;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static org.neo4j.graphdb.Label.label;

class SampleData
{
    static void createSomeData( int items, Cluster cluster ) throws Exception
    {
        for ( int i = 0; i < items; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( label( "boo" ) );
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }
    }

    static void createData( GraphDatabaseService db, int size )
    {
        for ( int i = 0; i < size; i++ )
        {
            Node node1 = db.createNode();
            Node node2 = db.createNode();

            node1.setProperty( "hej", "svej" );
            node2.setProperty( "tjabba", "tjena" );

            Relationship rel = node1.createRelationshipTo( node2, RelationshipType.withName( "halla" ) );
            rel.setProperty( "this", "that" );
        }
    }
}
