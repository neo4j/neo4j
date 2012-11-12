/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.pycompat;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ExecutionResult implements Iterable<Map<String,Object>>
{

    private org.neo4j.cypher.ExecutionResult inner;

    /**
     * Constructor used by the Cypher framework. End-users should not
     * create an ExecutionResult directly.
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
     *
     * @param n exact name of the column, as it appeared in the original query
     * @return an iterator of the result objects, possibly empty
     * @throws ClassCastException when the result object can not be cast to the requested type
     * @throws org.neo4j.graphdb.NotFoundException when the column name does not appear in the original query
     */
    public Iterator<Object> columnAs( String n )
    {
        return new WrappedIterator<Object>(inner.javaColumnAs( n ));
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
    public Iterator<Map<String, Object>> iterator()
    {
        return new WrappedIterator<Map<String, Object>>(inner.javaIterator());
    }

    @Override
    public String toString()
    {
        return inner.dumpToString();
    }

    public void toString( PrintWriter writer )
    {
        inner.dumpToString( writer );
    }

    public int length()
    {
        return inner.length();
    }

}
