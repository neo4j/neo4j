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
package org.neo4j.test.impl;

import java.util.List;

import static org.neo4j.helpers.Exceptions.launderedException;

public abstract class MultipleExceptionsStrategy
{
    public static void assertEmpty( List<Throwable> failures ) throws Throwable
    {
        if ( failures.isEmpty() ) return;
        if ( failures.size() == 1 ) throw failures.get( 0 );
        throw strategy.aggregate( failures );
    }

    public static void assertEmptyExceptions( List<? extends Exception> exceptions ) throws Exception
    {
        try
        {
            assertEmpty( unsafeCast(exceptions) );
        }
        catch ( Throwable e )
        {
            throw launderedException( Exception.class, e );
        }
    }

    private static final MultipleExceptionsStrategy strategy;
    static
    {
        MultipleExceptionsStrategy choice = new ChainedMultipleExceptionsStrategy();
        String pkg = MultipleExceptionsStrategy.class.getPackage().getName();
        ClassLoader loader = MultipleExceptionsStrategy.class.getClassLoader();
        for ( String name : new String[] { "JUnitMultipleExceptions" } )
        {
            try
            {
                choice = (MultipleExceptionsStrategy) loader.loadClass( pkg + "." + name ).newInstance();
            }
            catch ( Throwable e )
            {
                continue;
            }
            break;
        }
        strategy = choice;
    }

    abstract Throwable aggregate( List<Throwable> failures );

    MultipleExceptionsStrategy()
    {
        // subclasses live in this package
    }

    private static class ChainedMultipleExceptionsStrategy extends MultipleExceptionsStrategy
    {
        @Override
        Throwable aggregate( List<Throwable> failures )
        {
            Throwable result = null;
            for ( Throwable throwable : failures )
            {
                Throwable last = throwable;
                for ( Throwable cause = last.getCause(); cause != null; cause = cause.getCause() )
                {
                    last = cause;
                }
                last.initCause( result );
                result = throwable;
            }
            return null;
        }
    }

    @SuppressWarnings( "unchecked" )
    private static List<Throwable> unsafeCast( List<?> exceptions )
    {
        return (List<Throwable>)exceptions;
    }
}
