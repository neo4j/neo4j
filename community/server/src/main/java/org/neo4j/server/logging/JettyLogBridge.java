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
package org.neo4j.server.logging;

import org.eclipse.jetty.util.log.AbstractLogger;
import org.eclipse.jetty.util.log.Logger;

import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.logging.internal.LogMessageUtil.slf4jToStringFormatPlaceholders;

public class JettyLogBridge extends AbstractLogger
{
    private static final Pattern packagePattern = Pattern.compile( "(\\w)\\w+\\." );
    private static final AtomicReference<LogProvider> logProvider = new AtomicReference<>( NullLogProvider.getInstance() );
    private final String fullname;
    private final Log log;

    public JettyLogBridge()
    {
        this( "org.eclipse.jetty.util.log" );
    }

    public JettyLogBridge( String fullname )
    {
        this.fullname = fullname;
        this.log = logProvider.get().getLog( packagePattern.matcher( fullname ).replaceAll( "$1." ) );
    }

    public static LogProvider setLogProvider( LogProvider newLogProvider )
    {
        return logProvider.getAndSet( newLogProvider );
    }

    @Override
    protected Logger newLogger( String fullname )
    {
        return new JettyLogBridge( fullname );
    }

    @Override
    public String getName()
    {
        return fullname;
    }

    @Override
    public void warn( String msg, Object... args )
    {
        log.warn( slf4jToStringFormatPlaceholders( msg ), args );
    }

    @Override
    public void warn( Throwable thrown )
    {
        log.warn( "", thrown );
    }

    @Override
    public void warn( String msg, Throwable thrown )
    {
        log.warn( msg, thrown );
    }

    @Override
    public void info( String msg, Object... args )
    {
    }

    @Override
    public void info( Throwable thrown )
    {
    }

    @Override
    public void info( String msg, Throwable thrown )
    {
    }

    @Override
    public boolean isDebugEnabled()
    {
        return false;
    }

    @Override
    public void setDebugEnabled( boolean enabled )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void debug( String msg, Object... args )
    {
    }

    @Override
    public void debug( Throwable thrown )
    {
    }

    @Override
    public void debug( String msg, Throwable thrown )
    {
    }

    @Override
    public void ignore( Throwable ignored )
    {
    }
}
