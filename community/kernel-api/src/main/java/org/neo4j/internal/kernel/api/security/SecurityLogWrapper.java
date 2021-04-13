/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.kernel.api.security;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.logging.log4j.LogExtended;

public class SecurityLogWrapper implements SecurityLog
{
    LogExtended log;

    public SecurityLogWrapper( LogExtended log )
    {
        this.log = log;
    }

    @Override
    public void debug( String message )
    {
        log.debug( new SecurityLogLine( message ) );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        log.debug( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    @Override
    public void debug( LoginContext context, String message )
    {
        log.debug( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    @Override
    public void info( String message )
    {
        log.info( new SecurityLogLine( message ) );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        log.info( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    @Override
    public void info( ClientConnectionInfo connectionInfo, String message )
    {
        log.info( new SecurityLogLine( connectionInfo, message ) );
    }

    @Override
    public void info( LoginContext context, String message )
    {
        log.info( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    @Override
    public void warn( String message )
    {
        log.warn( new SecurityLogLine( message ) );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        log.warn( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    @Override
    public void warn( LoginContext context, String message )
    {
        log.warn( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    @Override
    public void error( String message )
    {
        log.error( new SecurityLogLine( message ) );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        log.error( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    @Override
    public void error( ClientConnectionInfo connectionInfo, String message )
    {
        log.error( new SecurityLogLine( connectionInfo, message ) );
    }

    @Override
    public void error( LoginContext context, String message )
    {
        log.error( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }
}
