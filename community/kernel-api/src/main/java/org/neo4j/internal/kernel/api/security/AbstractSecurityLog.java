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
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.log4j.StructureAwareMessage;

import static org.neo4j.internal.helpers.Strings.escape;

public abstract class AbstractSecurityLog extends LifecycleAdapter
{
    private InternalLog inner;

    public void setLog( InternalLog inner )
    {
        this.inner = inner;
    }

    public void debug( String message )
    {
        inner.debug( new SecurityLogLine( message ) );
    }

    public void debug( SecurityContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.debug( new SecurityLogLine( context.connectionInfo(), context.database(), subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void info( String message )
    {
        inner.info( new SecurityLogLine( message ) );
    }

    public void info( LoginContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.info( new SecurityLogLine( context.connectionInfo(), null, subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void info( SecurityContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.info( new SecurityLogLine( context.connectionInfo(), context.database(), subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void warn( String message )
    {
        inner.warn( new SecurityLogLine( message ) );
    }

    public void warn( SecurityContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.warn( new SecurityLogLine( context.connectionInfo(), context.database(), subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void error( String message )
    {
        inner.error( new SecurityLogLine( message ) );
    }

    public void error( ClientConnectionInfo connectionInfo, String message )
    {
        inner.error( new SecurityLogLine( connectionInfo, null, null, message, null ) );
    }

    public void error( LoginContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.error( new SecurityLogLine( context.connectionInfo(), null, subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void error( LoginContext context, String database, String message )
    {
        AuthSubject subject = context.subject();
        inner.error( new SecurityLogLine( context.connectionInfo(), database, subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public void error( SecurityContext context, String message )
    {
        AuthSubject subject = context.subject();
        inner.error( new SecurityLogLine( context.connectionInfo(), context.database(), subject.executingUser(), message, subject.authenticatedUser() ) );
    }

    public boolean isDebugEnabled()
    {
        return inner.isDebugEnabled();
    }

    static class SecurityLogLine extends StructureAwareMessage
    {
        private final String executingUser;
        private final String sourceString;
        private final String message;
        private final String authenticatedUser;
        private final String database;

        SecurityLogLine( String message )
        {
            this.sourceString = null;
            this.database = null;
            this.executingUser = null;
            this.authenticatedUser = null;
            this.message = message;
        }

        SecurityLogLine( ClientConnectionInfo connectionInfo, String database, String executingUser, String message, String authenticatedUser )
        {
            this.sourceString = connectionInfo.asConnectionDetails();
            this.database = database;
            this.executingUser = executingUser;
            // clean message of newlines
            this.message = message.replaceAll( "\\R+", " " );
            this.authenticatedUser = authenticatedUser;
        }

        @Override
        public void asString( StringBuilder sb )
        {
            if ( executingUser != null && executingUser.length() > 0 )
            {
                if ( executingUser.equals( authenticatedUser ) )
                {
                    sb.append( "[" ).append( escape( executingUser ) ).append( "]: " );
                }
                else
                {
                    sb.append( String.format( "[%s:%s]: ", escape( authenticatedUser ), escape( executingUser ) ) );
                }
            }
            sb.append( message );
        }

        @Override
        public void asStructure( FieldConsumer fieldConsumer )
        {
            fieldConsumer.add( "type", "security" );
            fieldConsumer.add( "source", sourceString );
            if ( database != null )
            {
                fieldConsumer.add( "database", database );
            }
            if ( executingUser != null && executingUser.length() > 0 )
            {
                fieldConsumer.add( "username", executingUser );
                fieldConsumer.add( "executingUser", executingUser );
            }
            if ( authenticatedUser != null && authenticatedUser.length() > 0 )
            {
                fieldConsumer.add( "authenticatedUser", authenticatedUser );
            }
            fieldConsumer.add( "message", message );
        }
    }
}
