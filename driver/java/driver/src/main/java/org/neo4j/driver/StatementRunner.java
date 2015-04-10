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
package org.neo4j.driver;

import java.util.Map;

/**
 * Common interface for components that can execute Neo4j statements.
 *
 * @see org.neo4j.driver.Session
 * @see org.neo4j.driver.Transaction
 */
public interface StatementRunner
{
    /**
     * Run a statement and return a result stream.
     * <p>
     * This method takes a set of parameters that will be injected into the
     * statement by Neo4j. Using parameters is highly encouraged, it helps avoid
     * dangerous cypher injection attacks and improves database performance as
     * Neo4j can re-use query plans more often.
     * <p>
     * <h2>Example</h2>
     * <pre>
     * {@code
     * Result res = session.run( "MATCH (n) WHERE n.name = {myNameParam} RETURN (n)",
     *              Neo4j.parameters( "myNameParam", "Bob" ) );
     * }
     * </pre>
     *
     * @param statement a Neo4j statement
     * @param parameters input data for the statement, see {@link org.neo4j.Neo4j#parameters(Object[])}
     * @return a stream of result values and associated metadata
     */
    Result run( String statement, Map<String,Value> parameters );

    /**
     * Run a statement and return a result stream.
     *
     * @param statement a Neo4j statement
     * @return a stream of result values and associated metadata
     */
    Result run( String statement );

}
