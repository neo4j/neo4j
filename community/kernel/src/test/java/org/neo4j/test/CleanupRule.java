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
package org.neo4j.test;

import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;

/**
 * Simple means of cleaning up after a test. It has two purposes:
 * o remove try-finally blocks from tests, which looks mostly like clutter
 * o remove @After code needing to clean up after a test
 *
 * Usage:
 * <pre><code>
 * public final @Rule CleanupRule cleanup = new CleanupRule();
 * ...
 * @Test
 * public void shouldAssertSomething()
 * {
 *     SomeObjectThatNeedsClosing dirt = cleanup.add( new SomeObjectThatNeedsClosing() );
 *     ...
 * }
 * </code></pre>
 *
 * It accepts {@link AutoCloseable} objects, since it's the lowest denominator for closeables.
 * And it accepts just about any object, where it tries to spot an appropriate close method, like "close" or "shutdown"
 * and calls it via reflection.
 */
public class CleanupRule extends ExternalResource
{
    private static final String[] COMMON_CLOSE_METHOD_NAMES = {"close", "stop", "shutdown", "shutDown"};
    private final LinkedList<AutoCloseable> toCloseAfterwards = new LinkedList<>();

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
                throw new RuntimeException( e );
            }
        }
    }

    public <T extends AutoCloseable> T add( T toClose )
    {
        toCloseAfterwards.addFirst( toClose );
        return toClose;
    }

    public <T> T add( T toClose )
    {
        Class<?> cls = toClose.getClass();
        for ( String methodName : COMMON_CLOSE_METHOD_NAMES )
        {
            try
            {
                Method method = cls.getMethod( methodName );
                method.setAccessible( true );
                add( closeable( method, toClose ) );
                return toClose;
            }
            catch ( NoSuchMethodException e )
            {
                // Try the next method
                continue;
            }
            catch ( SecurityException e )
            {
                throw new RuntimeException( e );
            }
        }
        throw new IllegalArgumentException( "No suitable close method found on " + toClose +
                ", which is a " + cls );
    }

    private AutoCloseable closeable( final Method method, final Object target )
    {
        return new AutoCloseable()
        {
            @Override
            public void close() throws IOException
            {
                try
                {
                    method.invoke( target );
                }
                catch ( IllegalAccessException | IllegalArgumentException | InvocationTargetException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }
}
