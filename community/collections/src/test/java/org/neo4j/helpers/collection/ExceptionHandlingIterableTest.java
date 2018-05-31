/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.helpers.collection;

import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertThrows;

@SuppressWarnings( "unchecked" )
class ExceptionHandlingIterableTest
{

    @Test
    void testHandleExceptionOnIteratorCreation()
    {
        assertThrows( IllegalStateException.class, () -> Iterables.count( new ExceptionHandlingIterable( () ->
        {
            throw new RuntimeException( "exception on iterator" );
        } )
        {
            @Override
            protected Iterator exceptionOnIterator( Throwable t )
            {
                rethrow( new IllegalStateException() );
                return super.exceptionOnIterator( t );
            }
        } ) );
    }

    @Test
    void testHandleExceptionOnNext()
    {
        assertThrows( IllegalStateException.class, () ->
                Iterables.count( new ExceptionHandlingIterable( () -> new Iterator()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return true;
                    }

                    @Override
                    public Object next()
                    {
                        throw new RuntimeException( "exception on next" );
                    }

                    @Override
                    public void remove()
                    {
                    }
                } )
                {
                    @Override
                    protected Object exceptionOnNext( Throwable t )
                    {
                        rethrow( new IllegalStateException() );
                        return super.exceptionOnNext( t );
                    }
                } ) );
    }

    @Test
    void testHandleExceptionOnHasNext()
    {
        assertThrows( IllegalStateException.class, () ->
                Iterables.count( new ExceptionHandlingIterable( () -> new Iterator()
                {
                    @Override
                    public boolean hasNext()
                    {
                        throw new RuntimeException( "exception on next" );
                    }

                    @Override
                    public Object next()
                    {
                        return null;
                    }

                    @Override
                    public void remove()
                    {
                    }
                } )
                {
                    @Override
                    protected boolean exceptionOnHasNext( Throwable t )
                    {
                        rethrow( new IllegalStateException() );
                        return super.exceptionOnHasNext( t );
                    }
                } ) );
    }
}
