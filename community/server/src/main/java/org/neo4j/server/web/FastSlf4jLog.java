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
package org.neo4j.server.web;

import org.eclipse.jetty.util.log.Logger;
import org.eclipse.jetty.util.log.Slf4jLog;

/**
 * Slf4jLog.isDebugEnabled delegates in the end to Logback, and since this method is called a lot and that method
 * is relatively slow, it has a big impact on the overall performance. This subclass fixes that by calling
 * isDebugEnabled
 * on creation, and then caches that.
 */
public class FastSlf4jLog
        extends Slf4jLog
{
    private boolean debugEnabled;

    public FastSlf4jLog() throws Exception
    {
        this( "org.eclipse.jetty.util.log" );
    }

    public FastSlf4jLog( String name )
    {
        super( name );

        debugEnabled = super.isDebugEnabled();
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Override
    public void debug( String msg, Object... args )
    {
        if ( debugEnabled )
        {
            if ( args != null && args.length == 0 )
            {
                args = null;
            }

            super.debug( msg, args );
        }
    }

    @Override
    public void debug( Throwable thrown )
    {
        if ( debugEnabled )
        {
            super.debug( thrown );
        }
    }

    @Override
    public void debug( String msg, Throwable thrown )
    {
        if ( debugEnabled )
        {
            super.debug( msg, thrown );
        }
    }

    protected Logger newLogger( String fullname )
    {
        return new FastSlf4jLog( fullname );
    }
}
