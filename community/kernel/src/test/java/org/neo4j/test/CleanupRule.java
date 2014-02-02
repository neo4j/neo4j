/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.test;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.junit.rules.ExternalResource;

public class CleanupRule extends ExternalResource
{
    private final List<AutoCloseable> toCloseAfterwards = new ArrayList<>();

    @Override
    protected void after()
    {
        for ( AutoCloseable toClose : toCloseAfterwards )
        {
            try
            {
                toClose.close();
            }
            catch ( Exception e )
            {
                System.out.println( "Couldn't clean up " + toClose + " after test finished" );
            }
        }
    }

    public <t> t add( t toClose )
    {
        toCloseAfterwards.add( closeableOf( toClose ) );
        return toClose;
    }

    private AutoCloseable closeableOf( final Object toClose )
    {
        if ( toClose instanceof AutoCloseable )
        {
            return (Closeable) toClose;
        }

        // Fallback to likely close methods by name
        for ( String methodName : new String[] { "close", "shutdown" } )
        {
            try
            {
                final Method method = toClose.getClass().getMethod( methodName );
                return new AutoCloseable()
                {
                    @Override
                    public void close() throws Exception
                    {
                        method.invoke( toClose );
                    }
                };
            }
            catch ( NoSuchMethodException | SecurityException e )
            {
                continue;
            }
        }

        throw new IllegalArgumentException( "Couldn't find a way to close " + toClose );
    }
}
