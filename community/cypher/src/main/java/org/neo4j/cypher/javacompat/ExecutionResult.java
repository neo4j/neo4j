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
package org.neo4j.cypher.javacompat;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Holds Cypher query result sets.
 */
public class ExecutionResult implements Iterable<Map<String,Object>>
{
    private org.neo4j.cypher.ExecutionResult inner;

    public ExecutionResult( org.neo4j.cypher.ExecutionResult projection )
    {
        inner = projection;
    }

    public <T> Iterator<T> columnAs( String n )
    {
        return inner.javaColumnAs( n );
    }

    public List<String> columns()
    {
        return inner.javaColumns();
    }

    @Override
    public Iterator<Map<String, Object>> iterator()
    {
        return inner.javaIterator();
    }

    @Override
    public String toString()
    {
        return inner.dumpToString();
    }
}
