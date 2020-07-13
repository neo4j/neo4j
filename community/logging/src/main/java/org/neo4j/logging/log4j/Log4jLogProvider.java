/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.logging.log4j;

import java.io.Closeable;
import java.io.OutputStream;

import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * A {@link LogProvider} implementation that uses the Log4j configuration ctx is connected to.
 */
public class Log4jLogProvider implements LogProvider, Closeable
{
    private final Neo4jLoggerContext ctx;

    public Log4jLogProvider( Neo4jLoggerContext ctx )
    {
        this.ctx = ctx;
    }

    public Log4jLogProvider( OutputStream out )
    {
        this( out, Level.INFO );
    }

    public Log4jLogProvider( OutputStream out, Level level )
    {
        this( LogConfig.createBuilder( out, level ).build() );
    }

    public void updateLogLevel( Level newLevel )
    {
        LogConfig.updateLogLevel( newLevel, ctx );
    }

    @Override
    public Log getLog( Class<?> loggingClass )
    {
        return new Log4jLog( ctx.getLogger( loggingClass.getName() ) );
    }

    @Override
    public Log getLog( String name )
    {
        return new Log4jLog( ctx.getLogger( name ) );
    }

    @Override
    public void close()
    {
        ctx.close();
    }
}
