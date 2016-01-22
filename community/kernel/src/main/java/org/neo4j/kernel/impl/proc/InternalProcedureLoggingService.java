/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.proc;

import org.neo4j.logging.Log;

public class InternalProcedureLoggingService implements LoggingService
{
    private final Log log;

    public InternalProcedureLoggingService( Log log )
    {
        this.log = log;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public void debug( String message )
    {
        log.debug( message );
    }

    @Override
    public void debug( String message, Object... arguments )
    {
        log.debug( message, arguments );
    }

    @Override
    public void debug( String message, Throwable throwable )
    {
        log.debug( message, throwable );
    }

    @Override
    public void info( String message )
    {
        log.info( message );
    }

    @Override
    public void info( String message, Object... arguments )
    {
        log.info( message, arguments );
    }

    @Override
    public void info( String message, Throwable throwable )
    {
        log.info( message, throwable );
    }

    @Override
    public void warn( String message )
    {
        log.warn( message );
    }

    @Override
    public void warn( String message, Object... arguments )
    {
        log.warn( message, arguments );
    }

    @Override
    public void warn( String message, Throwable throwable )
    {
        log.warn( message, throwable );
    }

    @Override
    public void error( String message )
    {
        log.error( message );
    }

    @Override
    public void error( String message, Object... arguments )
    {
        log.error( message, arguments );
    }

    @Override
    public void error( String message, Throwable throwable )
    {
        log.error( message, throwable );
    }
}
