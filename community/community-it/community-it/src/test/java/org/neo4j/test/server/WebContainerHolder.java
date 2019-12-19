/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.test.server;

import org.neo4j.server.helpers.CommunityWebContainerBuilder;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.helpers.WebContainerHelper;

final class WebContainerHolder extends Thread
{
    private static AssertionError allocation;
    private static TestWebContainer testWebContainer;
    private static CommunityWebContainerBuilder builder;

    static synchronized TestWebContainer allocate() throws Exception
    {
        if ( allocation != null )
        {
            throw allocation;
        }
        if ( testWebContainer == null )
        {
            testWebContainer = startServer();
        }
        allocation = new AssertionError( "The server was allocated from here but not released properly" );
        return testWebContainer;
    }

    static synchronized void release( TestWebContainer server )
    {
        if ( server == null )
        {
            return;
        }
        if ( server != WebContainerHolder.testWebContainer )
        {
            throw new AssertionError( "trying to suspend a server not allocated from here" );
        }
        if ( allocation == null )
        {
            throw new AssertionError( "releasing the server although it is not allocated" );
        }
        allocation = null;
    }

    static synchronized void ensureNotRunning()
    {
        if ( allocation != null )
        {
            throw allocation;
        }
        shutdown();
    }

    static synchronized void setWebContainerBuilderProperty( String key, String value )
    {
        initBuilder();
        builder = builder.withProperty( key, value );
    }

    private static TestWebContainer startServer() throws Exception
    {
        initBuilder();
        return WebContainerHelper.createNonPersistentContainer( builder );
    }

    private static synchronized void shutdown()
    {
        allocation = null;
        try
        {
            if ( testWebContainer != null )
            {
                testWebContainer.shutdown();
            }
        }
        finally
        {
            builder = null;
            testWebContainer = null;
        }
    }

    private static void initBuilder()
    {
        if ( builder == null )
        {
            builder = CommunityWebContainerBuilder.builder();
        }
    }

    @Override
    public void run()
    {
        shutdown();
    }
}
