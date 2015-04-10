/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.example;

import org.neo4j.Neo4j;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;

import static org.neo4j.Neo4j.parameters;

/**
 * This shows an example of nested result iteration.
 */
public class NestedResultExample
{
    public static void main( String... args )
    {
        // tag::example[]
        Session session = Neo4j.session( "neo4j+http://localhost:7687" );

        for ( Record outer : session.run( "MATCH (n) RETURN id(n) AS node_id" ).retain() )
        {
            long nodeID = outer.get( "node_id" ).javaLong();
            for ( Record inner : session.run(
                    "MATCH (n) WHERE ID(n) = {i} RETURN n.name AS name",
                    parameters( "i", nodeID ) ).retain() )
            {
                System.out.println( inner.get( "name" ) );
            }
        }

        session.close();
        // end::example[]
    }
}
