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
package org.neo4j.bolt.v3.messaging;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.v3.messaging.request.BeginMessage;
import org.neo4j.bolt.v3.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.InterruptSignal;
import org.neo4j.bolt.v3.messaging.request.PullAllMessage;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarksParserV3;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

/**
 * Quick access of all messages of Bolt Protocol V3
 */
public class BoltV3Messages
{
    private static final String USER_AGENT = "BoltV3Messages/0.0";
    private static final RequestMessage HELLO = new HelloMessage( map( "user_agent", USER_AGENT ) );
    private static final RequestMessage RUN_RETURN_ONE = new RunMessage( "RETURN 1" );
    private static final RequestMessage BEGIN = new BeginMessage();

    public static Stream<RequestMessage> supported()
    {
        return Stream.of( hello(), goodbye(), run(), discardAll(), pullAll(), begin(), commit(), rollback(), reset() );
    }

    public static Stream<RequestMessage> unsupported() throws BoltIOException
    {
        return Stream.of(
                // bolt v4 messages new message types
                BoltV4Messages.pull( 10 ),
                BoltV4Messages.discard(10 )
        );
    }

    public static RequestMessage hello()
    {
        return HELLO;
    }

    public static RequestMessage goodbye()
    {
        return GOODBYE_MESSAGE;
    }

    public static RequestMessage run()
    {
        return RUN_RETURN_ONE;
    }

    public static RequestMessage run( String statement )
    {
        return new RunMessage( statement );
    }

    public static RequestMessage discardAll()
    {
        return DiscardAllMessage.INSTANCE;
    }

    public static RequestMessage pullAll()
    {
        return PullAllMessage.INSTANCE;
    }

    public static RequestMessage begin()
    {
        return BEGIN;
    }

    public static RequestMessage begin( MapValue bookmarks ) throws BoltIOException
    {
        var bookmarkList = BookmarksParserV3.INSTANCE.parseBookmarks( bookmarks );
        return new BeginMessage( MapValue.EMPTY, bookmarkList, null, AccessMode.WRITE, Map.of() );
    }

    public static RequestMessage commit()
    {
        return COMMIT_MESSAGE;
    }

    public static RequestMessage rollback()
    {
        return ROLLBACK_MESSAGE;
    }

    public static RequestMessage reset()
    {
        return ResetMessage.INSTANCE;
    }

    /**
     * Internal message, can never be sent from clients
     */
    public static RequestMessage interrupt()
    {
        return InterruptSignal.INSTANCE;
    }
}
