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
package org.neo4j.graphdb;

import java.util.Iterator;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.util.PrimitiveIntIterator;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveIntIterator;
import static org.neo4j.helpers.collection.IteratorUtil.toPrimitiveLongIterator;

public class Neo4jMockitoHelpers
{
    public static <T> Answer<Iterator<T>> answerAsIteratorFrom( final Iterable<T> values )
    {
        return new Answer<Iterator<T>>()
        {
            @Override
            public Iterator<T> answer( InvocationOnMock invocation ) throws Throwable
            {
                return values.iterator();
            }
        };
    }

    public static Answer<PrimitiveLongIterator> answerAsPrimitiveLongIteratorFrom( final Iterable<Long> values )
    {
        return new Answer<PrimitiveLongIterator>()
        {
            @Override
            public PrimitiveLongIterator answer( InvocationOnMock invocation ) throws Throwable
            {
                return toPrimitiveLongIterator( values.iterator() );
            }
        };
    }

    public static Answer<PrimitiveIntIterator> answerAsPrimitiveIntIteratorFrom( final Iterable<Integer> values )
    {
        return new Answer<PrimitiveIntIterator>()
        {
            @Override
            public PrimitiveIntIterator answer( InvocationOnMock invocation ) throws Throwable
            {
                return toPrimitiveIntIterator( values.iterator() );
            }
        };
    }
}
