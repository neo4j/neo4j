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
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.log4j.LogExtended;
import org.neo4j.logging.log4j.StructureAwareMessage;

import static org.neo4j.internal.helpers.Strings.escape;

public abstract class AbstractSecurityLog extends LifecycleAdapter
{
    LogExtended inner;

    public void setLog( LogExtended inner )
    {
        this.inner = inner;
    }

    public void debug( String message )
    {
        inner.debug( new SecurityLogLine( message ) );
    }

    public void debug( String format, Object... arguments )
    {
        inner.debug( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    public void debug( LoginContext context, String message )
    {
        inner.debug( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    public void info( String message )
    {
        inner.info( new SecurityLogLine( message ) );
    }

    public void info( String format, Object... arguments )
    {
        inner.info( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    public void info( LoginContext context, String message )
    {
        inner.info( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    public void info( ClientConnectionInfo connectionInfo, String message )
    {
        inner.info( new SecurityLogLine( connectionInfo, message ) );
    }

    public void warn( String message )
    {
        inner.warn( new SecurityLogLine( message ) );
    }

    public void warn( LoginContext context, String message )
    {
        inner.warn( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    public void error( String message )
    {
        inner.error( new SecurityLogLine( message ) );
    }

    public void error( String format, Object... arguments )
    {
        inner.error( new SecurityLogLine( String.format( format, arguments ) ) );
    }

    public void error( ClientConnectionInfo connectionInfo, String message )
    {
        inner.error( new SecurityLogLine( connectionInfo, message ) );
    }

    public void error( LoginContext context, String message )
    {
        inner.error( new SecurityLogLine( context.subject().username(), context.connectionInfo(), message ) );
    }

    public boolean isDebugEnabled()
    {
        return inner.isDebugEnabled();
    }

    static class SecurityLogLine extends StructureAwareMessage
    {
        private final String username;
        private final String sourceString;
        private final String message;

        SecurityLogLine( String message )
        {
            this.username = null;
            this.sourceString = null;
            this.message = message;
        }

        SecurityLogLine( ClientConnectionInfo connectionInfo, String message )
        {
            this( null, connectionInfo, message );
        }

        SecurityLogLine( String username, ClientConnectionInfo connectionInfo, String message )
        {
            this.username = username;
            this.sourceString = connectionInfo.asConnectionDetails();
            // clean message of newlines
            this.message = message.replaceAll( "\\R+", " " );
        }

        @Override
        public void asString( StringBuilder sb )
        {
            if ( username != null && username.length() > 0 )
            {
                sb.append( "[" ).append( escape( username ) ).append( "]: " );
            }
            sb.append( message );
        }

        @Override
        public void asStructure( FieldConsumer fieldConsumer )
        {
            fieldConsumer.add( "type", "security" );
            fieldConsumer.add( "source", sourceString );
            fieldConsumer.add( "username", username );
            fieldConsumer.add( "message", message );
        }
    }
}
