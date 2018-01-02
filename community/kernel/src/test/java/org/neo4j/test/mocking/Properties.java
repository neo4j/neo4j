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
package org.neo4j.test.mocking;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.test.Property;

public class Properties implements Answer<Object>, Iterable<String>
{
    public static Properties properties( Property... properties )
    {
        return new Properties( properties );
    }

    public static Properties properties( Map<String, Object> properties )
    {
        return new Properties( properties );
    }

    private final SortedMap<String, Object> properties = new TreeMap<>();

    private Properties( Property[] properties )
    {
        for ( Property property : properties )
        {
            this.properties.put( property.key(), property.value() );
        }
    }

    private Properties( Map<String, Object> properties )
    {
        this.properties.putAll( properties );
    }

    @Override
    public Object answer( InvocationOnMock invocation ) throws Throwable
    {
        Object[] arguments = invocation.getArguments();
        @SuppressWarnings("SuspiciousMethodCalls")
        Object result = properties.get( arguments[0] );
        if ( result == null )
        {
            if ( arguments.length == 2 )
            {
                return arguments[1];
            }
            else
            {
                throw new NotFoundException();
            }
        }
        return result;
    }

    @Override
    public Iterator<String> iterator()
    {
        return properties.keySet().iterator();
    }

    public SortedMap<String, Object> getProperties()
    {
        return properties;
    }
}
