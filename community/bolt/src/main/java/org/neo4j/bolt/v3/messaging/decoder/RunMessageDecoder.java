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
package org.neo4j.bolt.v3.messaging.decoder;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.v3.runtime.bookmarking.BookmarksParserV3;
import org.neo4j.bolt.v3.messaging.request.MessageMetadataParser;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.bolt.v3.messaging.request.RunMessage.SIGNATURE;

public class RunMessageDecoder implements RequestMessageDecoder
{
    private final BoltResponseHandler responseHandler;
    private final BookmarksParser bookmarksParser;

    public RunMessageDecoder( BoltResponseHandler responseHandler )
    {
        this( responseHandler, BookmarksParserV3.INSTANCE );
    }

    protected RunMessageDecoder( BoltResponseHandler responseHandler, BookmarksParser bookmarksParser )
    {
        this.responseHandler = responseHandler;
        this.bookmarksParser = bookmarksParser;
    }

    @Override
    public int signature()
    {
        return SIGNATURE;
    }

    @Override
    public BoltResponseHandler responseHandler()
    {
        return responseHandler;
    }

    @Override
    public RequestMessage decode( Neo4jPack.Unpacker unpacker ) throws IOException
    {
        var statement = unpacker.unpackString();
        var params = unpacker.unpackMap();
        var metadata = unpacker.unpackMap();
        var bookmarks = bookmarksParser.parseBookmarks( metadata );
        var txTimeout = MessageMetadataParser.parseTransactionTimeout( metadata );
        var accessMode = MessageMetadataParser.parseAccessMode( metadata );
        var txMetadata = MessageMetadataParser.parseTransactionMetadata( metadata );

        return newRunMessage( statement, params, metadata, bookmarks, txTimeout, accessMode, txMetadata );
    }

    protected RequestMessage newRunMessage( String statement, MapValue params, MapValue meta, List<Bookmark> bookmarks, Duration txTimeout,
            AccessMode accessMode, Map<String,Object> txMetadata ) throws BoltIOException
    {
        return new RunMessage( statement, params, meta, bookmarks, txTimeout, accessMode, txMetadata );
    }
}

