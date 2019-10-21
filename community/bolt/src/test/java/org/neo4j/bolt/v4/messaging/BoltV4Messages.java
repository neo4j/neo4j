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
package org.neo4j.bolt.v4.messaging;

import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.v3.messaging.BoltV3Messages;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;
import org.neo4j.bolt.v3.messaging.request.InterruptSignal;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarksParserV4;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.GoodbyeMessage.GOODBYE_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

/**
 * Quick access of all Bolt V4 messages
 */
public class BoltV4Messages
{
    private static final String USER_AGENT = "BoltV4Messages/0.0";
    private static final RequestMessage HELLO = new HelloMessage( map( "user_agent", USER_AGENT ) );
    private static final RequestMessage RUN_RETURN_ONE = new RunMessage( "RETURN 1" );
    private static final RequestMessage BEGIN = new BeginMessage();

    public static Stream<RequestMessage> supported() throws BoltIOException
    {
        return Stream.of( hello(), goodbye(), run(), discard( 10 ), pull( 10 ), begin(), commit(), rollback(), reset() );
    }

    public static Stream<RequestMessage> unsupported() throws BoltIOException
    {
        return Stream.of( // bolt v3 only messages
                BoltV3Messages.pullAll(), BoltV3Messages.discardAll() );

    }

    public static RequestMessage begin() throws BoltIOException
    {
        return BEGIN;
    }

    public static RequestMessage begin( DatabaseIdRepository repository, MapValue bookmarks ) throws BoltIOException
    {
        var bookmarkList = new BookmarksParserV4( repository, CustomBookmarkFormatParser.DEFAULT ).parseBookmarks( bookmarks );
        return new BeginMessage( MapValue.EMPTY, bookmarkList, null, AccessMode.WRITE, Map.of(), ABSENT_DB_NAME );
    }

    public static RequestMessage discard( long n ) throws BoltIOException
    {
        return new DiscardMessage( asMapValue( map( "n", n ) ) );
    }

    public static RequestMessage pull( long n ) throws BoltIOException
    {
        return new PullMessage( asMapValue( map( "n", n ) ) );
    }

    public static RequestMessage hello()
    {
        return HELLO;
    }

    public static RequestMessage hello( Map<String,Object> meta )
    {
        if ( !meta.containsKey( "user_agent" ) )
        {
            meta.put( "user_agent", USER_AGENT );
        }
        return new HelloMessage( meta );
    }

    public static RequestMessage run( String statement )
    {
        return new RunMessage( statement );
    }

    public static RequestMessage run()
    {
        return RUN_RETURN_ONE;
    }

    public static RequestMessage pullAll() throws BoltIOException
    {
        return pull( -1 );
    }

    public static RequestMessage discardAll() throws BoltIOException
    {
        return discard( -1 );
    }

    public static RequestMessage run( String statement, MapValue params )
    {
        return new RunMessage( statement, params );
    }

    public static RequestMessage run( String statement, MapValue params, MapValue meta )
    {
        return new RunMessage( statement, params, meta );
    }

    public static RequestMessage rollback()
    {
        return ROLLBACK_MESSAGE;
    }

    public static RequestMessage commit()
    {
        return COMMIT_MESSAGE;
    }

    public static RequestMessage reset()
    {
        return ResetMessage.INSTANCE;
    }

    public static RequestMessage goodbye()
    {
        return GOODBYE_MESSAGE;
    }

    public static RequestMessage Interrupted()
    {
        // This is a special state machine internal message.
        // A client shall never send this to server
        return InterruptSignal.INSTANCE;
    }
}
