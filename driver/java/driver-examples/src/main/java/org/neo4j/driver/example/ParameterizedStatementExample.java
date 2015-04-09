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
import org.neo4j.driver.Session;
import org.neo4j.driver.Result;

/**
 * This shows you how to use parameterized statements with Neo4j. Unless your queries are fixed, static strings, you
 * should always use parameters. It helps improve performance significantly, and it is an important security measure.
 */
public class ParameterizedStatementExample
{
    public static void main(String ... args)
    {
        // First, establish a session, same as in the basic example
        Session session = Neo4j.session( "neo4j+http://localhost:7687" );

        // Running a statement with parameters where we don't care about the result:
        session.run( "CREATE (n {name:{name}})", Neo4j.parameters( "name", "Bob" ) );

        // Running a statement with parameters, and iterating over the result stream
        Result result = session.run(
                "MATCH (n) WHERE n.name = {name} RETURN n.name", Neo4j.parameters( "name", "Bob" ) );
        while(result.next())
        {
            System.out.println( result.get( "n.name" ));
        }

        // And, as explained in the basic example, always close sessions when you are done with them.
        session.close();
    }
}
