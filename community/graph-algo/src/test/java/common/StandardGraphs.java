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
package common;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public enum StandardGraphs implements GraphDefinition, RelationshipType
{
    CROSS_PATHS_GRAPH
    {
        public Node create( GraphDatabaseService graphdb )
        {
            Node start = graphdb.createNode(), end = graphdb.createNode();
            Node a = graphdb.createNode(), b = graphdb.createNode(), c = graphdb.createNode(), d = graphdb.createNode();
            start.createRelationshipTo( a, this );
            start.createRelationshipTo( b, this );
            a.createRelationshipTo( c, this );
            a.createRelationshipTo( d, this );
            b.createRelationshipTo( c, this );
            b.createRelationshipTo( d, this );
            c.createRelationshipTo( end, this );
            d.createRelationshipTo( end, this );
            return end;
        }
    },
    SMALL_CIRCLE
    {
        public Node create( GraphDatabaseService graphdb )
        {
            Node start = graphdb.createNode(), end = graphdb.createNode();
            start.createRelationshipTo( end, this );
            end.createRelationshipTo( start, this );
            return end;
        }
    },
    MATRIX_EXAMPLE
    {

        public Node create( GraphDatabaseService graphdb )
        {
            Node neo = graphdb.createNode();
            neo.setProperty( "name", "Thomas Anderson" );
            neo.setProperty( "age", 29 );

            Node trinity = graphdb.createNode();
            trinity.setProperty( "name", "Trinity" );

            Node morpheus = graphdb.createNode();
            morpheus.setProperty( "name", "Morpheus" );
            morpheus.setProperty( "rank", "Captain" );
            morpheus.setProperty( "occupation", "Total badass" );

            Node cypher = graphdb.createNode();
            cypher.setProperty( "name", "Cypher" );
            cypher.setProperty( "last name", "Reagan" );

            Node smith = graphdb.createNode();
            smith.setProperty( "name", "Agent Smith" );
            smith.setProperty( "version", "1.0b" );
            smith.setProperty( "language", "C++" );

            Node architect = graphdb.createNode();
            architect.setProperty( "name", "The Architect" );

            Relationship relationship;

            relationship = neo.createRelationshipTo( morpheus,
                    StandardGraphs.MatrixTypes.KNOWS );
            relationship = neo.createRelationshipTo( trinity,
                    StandardGraphs.MatrixTypes.KNOWS );
            relationship = morpheus.createRelationshipTo( trinity,
                    StandardGraphs.MatrixTypes.KNOWS );
            relationship.setProperty( "since", "a year before the movie" );
            relationship.setProperty( "cooporates on", "The Nebuchadnezzar" );
            relationship = trinity.createRelationshipTo( neo,
                    StandardGraphs.MatrixTypes.LOVES );
            relationship.setProperty( "since", "meeting the oracle" );
            relationship = morpheus.createRelationshipTo( cypher,
                    StandardGraphs.MatrixTypes.KNOWS );
            relationship.setProperty( "disclosure", "public" );
            relationship = cypher.createRelationshipTo( smith,
                    StandardGraphs.MatrixTypes.KNOWS );
            relationship.setProperty( "disclosure", "secret" );
            relationship = smith.createRelationshipTo( architect,
                    StandardGraphs.MatrixTypes.CODED_BY );

            return neo;
        }
    };
    public enum MatrixTypes implements RelationshipType
    {
        KNOWS,
        CODED_BY,
        LOVES
    }
}
