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
package org.neo4j.bolt.v44.messaging.decoder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.runtime.BookmarksParser;
import org.neo4j.bolt.v44.messaging.request.BeginMessage;
import org.neo4j.values.virtual.MapValue;

public class BeginMessageDecoder extends org.neo4j.bolt.v4.messaging.BeginMessageDecoder {

    public BeginMessageDecoder(BoltResponseHandler responseHandler, BookmarksParser bookmarksParser) {
        super(responseHandler, bookmarksParser);
    }

    @Override
    protected RequestMessage newBeginMessage(
            MapValue metadata,
            List<Bookmark> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName)
            throws BoltIOException {
        var impersonatedUser = MessageMetadataParser.parseImpersonatedUser(metadata);
        return new BeginMessage(
                metadata,
                bookmarks,
                txTimeout,
                accessMode,
                txMetadata,
                databaseName,
                impersonatedUser); // v4.4 Begin Message
    }
}
