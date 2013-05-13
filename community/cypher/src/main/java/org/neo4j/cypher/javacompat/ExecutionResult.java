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
package org.neo4j.cypher.javacompat;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
public class ExecutionResult implements Iterable<Map<String,Object>>
{
    private org.neo4j.cypher.ExecutionResult inner;

    /**
     * Constructor used by the Cypher framework. End-users should not
     * create an ExecutionResult directly, but instead use the result
     * returned from calling {@link ExecutionEngine#execute(String)}.
     *
     * @param projection
     */
    public ExecutionResult( org.neo4j.cypher.ExecutionResult projection )
    {
        inner = projection;
    }

    /**
     * Provides result objects from a single column of the result set. This method is best used for
     * single column results.
     * <p>
     * <b>The Iterator returned must be fully exhausted to ensure that any resources, including transactions, bound
     * to it, are properly closed.</b>
     * </p>
     *
     * @param n exact name of the column, as it appeared in the original query
     * @param <T> desired type cast for the result objects
     * @return an iterator of the result objects, possibly empty
     * @throws ClassCastException when the result object can not be cast to the requested type
     * @throws org.neo4j.graphdb.NotFoundException when the column name does not appear in the original query
     */
    public <T> Iterator<T> columnAs( String n )
    {
        return inner.javaColumnAs( n );
    }

    /**
     * The exact names used to represent each column in the result set.
     *
     * @return List of the column names.
     */
    public List<String> columns()
    {
        return inner.javaColumns();
    }

    @Override
    public String toString()
    {
        return inner.toString();
    }

    /**
     * Provides a textual representation of the query result.
     * <p><b>
     * The execution result represented by this object will be consumed in its entirety after this method is called.
     * Calling any of the other iterating methods on it should not be expected to return any results.
     * </b></p>
     * @return Returns the execution result
     */
    public String dumpToString()
    {
        return inner.dumpToString();
    }

    /**
     * Returns statistics about this result.
     * @return statistics about this result
     */
    public QueryStatistics getQueryStatistics()
    {
        return new QueryStatistics( inner.queryStatistics() );
    }

    /**
     * Returns a string representation of the query plan used to produce this result.
     * @return a string representation of the query plan used to produce this result.
     */
    public PlanDescription executionPlanDescription()
    {
        return inner.executionPlanDescription().asJava();
    }

    public void toString( PrintWriter writer )
    {
        inner.dumpToString( writer );
    }

    /**
     * Returns an iterator over the <i>return</i> clause of the query. The format is a map that has as keys the names
     * of the columns or their explicit names (set via 'as') and the value is the calculated value. Each iterator item
     * is one row of the query result.
     * <p>
     * <b>The Iterator returned must be fully exhausted to ensure that any resources, including transactions bound
     * to it, are properly closed.</b>
     * </p>
     *
     * @return An iterator over the result of the query as a map from projected column name to value
     */
    @Override
    public Iterator<Map<String, Object>> iterator()
    {
        return inner.javaIterator();
    }
}
