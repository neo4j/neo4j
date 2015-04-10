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

import java.util.Map;

import org.neo4j.Neo4j;
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;

/**
 * This shows a simple example of using Neo4j in your Java application.
 */
public class BasicDriverExample
{
    public static void main( String... args )
    {
        // tag::example[]
        Session session = Neo4j.session( "neo4j://localhost" );

        String statement = "CREATE (a {name:{n}}) RETURN a.name";
        Map<String, Value> parameters = Neo4j.parameters( "n", "Bob" );

        Result result = session.run( statement, parameters );
        while ( result.next() )
        {
            System.out.println( result.get( "a.name" ) );
        }

        session.close();
        // end::example[]
    }
}
