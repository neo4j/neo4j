/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import org.neo4j.cypher.javacompat.ExecutionResult;

/**
 * Holds Cypher query result sets, in tabular form. Each row of the result is a map
 * of column name to result object. Each column name correlates directly
 * with the terms used in the "return" clause of the Cypher query.
 * The result objects could be {@link org.neo4j.graphdb.Node Nodes},
 * {@link org.neo4j.graphdb.Relationship Relationships} or java primitives.
 *
 *
 * Either iterate directly over the ExecutionResult to retrieve each row of the result
 * set, or use <code>columnAs()</code> to access a single column with result objects
 * cast to a type.
 */
public class ExtendedExecutionResult extends ExecutionResult
{
    private final org.neo4j.cypher.ExtendedExecutionResult inner;

    public ExtendedExecutionResult( org.neo4j.cypher.ExtendedExecutionResult projection )
    {
        super( projection );
        inner = projection;
    }

    /**
     * @return Whether the query is requesting a plan description to be returned.
     */
    public boolean planDescriptionRequested()
    {
        return inner.planDescriptionRequested();
    }
}