/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.logging;

import org.mortbay.log.Logger;

/**
 * Simple wrapper over the neo4j native logger class for getting jetty to use
 * our logging framework. See <a
 * href="http://docs.codehaus.org/display/JETTY/Debugging">the Jetty Debug
 * page</a> for more info.
 * 
 * @author Chris Gioran
 * 
 */

public class JettyLoggerAdapter implements Logger
{
    private org.neo4j.server.logging.Logger delegate;

    public JettyLoggerAdapter()
    {
        delegate = org.neo4j.server.logging.Logger.getLogger( "SYSTEM" );
    }

    @Override
    public void debug( String arg0, Throwable arg1 )
    {
        delegate.debug( arg0, arg1 );
    }

    @Override
    public void debug( String arg0, Object arg1, Object arg2 )
    {
        delegate.debug( arg0, arg1, arg2 );
    }

    @Override
    public Logger getLogger( String arg0 )
    {
        JettyLoggerAdapter newInstance = new JettyLoggerAdapter();
        newInstance.delegate = org.neo4j.server.logging.Logger.getLogger( arg0 );
        return newInstance;
    }

    @Override
    public void info( String arg0, Object arg1, Object arg2 )
    {
        delegate.info( arg0, arg1, arg2 );

    }

    @Override
    public boolean isDebugEnabled()
    {
        return true;
    }

    @Override
    public void setDebugEnabled( boolean arg0 )
    {
    }

    @Override
    public void warn( String arg0, Throwable arg1 )
    {
        delegate.warn( arg1 );
    }

    @Override
    public void warn( String arg0, Object arg1, Object arg2 )
    {
        delegate.warn( arg0, arg1, arg2 );

    }
}
