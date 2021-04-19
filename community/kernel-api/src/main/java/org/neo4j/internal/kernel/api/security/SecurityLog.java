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
import org.neo4j.logging.log4j.StructureAwareMessage;

import static org.neo4j.internal.helpers.Strings.escape;

public interface SecurityLog
{
    void debug( String message );

    void debug( String format, Object... arguments );

    void debug( LoginContext context, String message );

    void info( String message );

    void info( String format, Object... arguments );

    void info( ClientConnectionInfo connectionInfo, String message );

    void info( LoginContext context, String message );

    void warn( String message );

    void warn( String format, Object... arguments );

    void warn( LoginContext context, String message );

    void error( String message );

    void error( String format, Object... arguments );

    void error( ClientConnectionInfo connectionInfo, String message );

    void error( LoginContext context, String message );

    class SecurityLogLine extends StructureAwareMessage
    {
        private final String username;
        private final String sourceString;
        private final String message;

        public SecurityLogLine( String message )
        {
            this.username = null;
            this.sourceString = null;
            this.message = message;
        }

        public SecurityLogLine( ClientConnectionInfo connectionInfo, String message )
        {
            this( null, connectionInfo, message );
        }

        public SecurityLogLine( String username, ClientConnectionInfo connectionInfo, String message )
        {
            this.username = username;
            this.sourceString = connectionInfo.asConnectionDetails();
            this.message = message;
        }

        @Override
        public void asString( StringBuilder sb )
        {
            if ( sourceString != null )
            {
                sb.append( sourceString ).append( "\t" );
            }
            if ( username != null )
            {
                sb.append( "[" ).append( escape( username ) ).append( "] : " );
            }
            else
            {
                sb.append( "[] : " );
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
