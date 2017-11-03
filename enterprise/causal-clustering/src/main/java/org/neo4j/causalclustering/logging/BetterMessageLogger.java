/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.logging;

import java.io.PrintWriter;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.lang.String.format;
import static java.lang.String.valueOf;

public class BetterMessageLogger<MEMBER> extends LifecycleAdapter implements MessageLogger<MEMBER>
{
    private enum Direction
    {
        INFO( "---" ),
        OUTBOUND( "-->" ),
        INBOUND( "<--" );

        public final String arrow;

        Direction( String arrow )
        {
            this.arrow = arrow;
        }
    }

    private final PrintWriter printWriter;
    private final Clock clock;
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern( "yyyy-MM-ddHH:mm:ss.SSSZ" );

    public BetterMessageLogger( MEMBER myself, PrintWriter printWriter, Clock clock )
    {
        this.printWriter = printWriter;
        this.clock = clock;
        log( myself, Direction.INFO, myself, "Info", "I am " + myself );
    }

    private void log( MEMBER me, Direction direction, MEMBER remote, String type, String message )
    {
        printWriter.println( format( "%s %s %s %s %s \"%s\"",
                ZonedDateTime.now( clock ).format( dateTimeFormatter ), me, direction.arrow, remote, type, message ) );
        printWriter.flush();
    }

    @Override
    public <M extends RaftMessages.RaftMessage> void logOutbound( MEMBER me, M message, MEMBER remote )
    {
        log( me, Direction.OUTBOUND, remote, nullSafeMessageType( message ), valueOf( message ) );
    }

    @Override
    public <M extends RaftMessages.RaftMessage> void logInbound( MEMBER remote, M message, MEMBER me )
    {
        log( me, Direction.INBOUND, remote, nullSafeMessageType( message ), valueOf( message ) );
    }

    private <M extends RaftMessages.RaftMessage> String nullSafeMessageType( M message )
    {
        if ( Objects.isNull( message ) )
        {
            return "null";
        }
        else
        {
            return message.type().toString();
        }
    }

    @Override
    public void stop()
    {
        printWriter.close();
    }
}
