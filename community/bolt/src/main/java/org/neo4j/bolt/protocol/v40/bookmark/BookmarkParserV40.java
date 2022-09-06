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
package org.neo4j.bolt.protocol.v40.bookmark;

import static java.lang.String.format;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarkParser;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkFormatException;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkParserException;
import org.neo4j.bolt.protocol.error.bookmark.InvalidBookmarkMixtureException;
import org.neo4j.bolt.protocol.error.bookmark.MalformedBookmarkException;
import org.neo4j.bolt.protocol.error.bookmark.UnknownDatabaseBookmarkException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;

public final class BookmarkParserV40 implements BookmarkParser {
    private static final long ABSENT_BOOKMARK_ID = -1L;

    private final DatabaseIdRepository databaseIdRepository;
    private final CustomBookmarkFormatParser customBookmarkFormatParser;

    public BookmarkParserV40(
            DatabaseIdRepository databaseIdRepository, CustomBookmarkFormatParser customBookmarkFormatParser) {
        this.databaseIdRepository = databaseIdRepository;
        this.customBookmarkFormatParser = customBookmarkFormatParser;
    }

    @Override
    public List<Bookmark> parseBookmarks(ListValue bookmarks) throws BookmarkParserException {
        var maxSystemDbTxId = ABSENT_BOOKMARK_ID;

        NamedDatabaseId userDbId = null;
        var maxUserDbTxId = ABSENT_BOOKMARK_ID;

        List<String> customBookmarkStrings = new ArrayList<>();

        for (var bookmark : bookmarks) {
            if (bookmark != Values.NO_VALUE) {
                var bookmarkString = toBookmarkString(bookmark);

                if (customBookmarkFormatParser.isCustomBookmark(bookmarkString)) {
                    customBookmarkStrings.add(bookmarkString);
                } else {
                    var parsedBookmark = parse(bookmarkString);

                    if (NAMED_SYSTEM_DATABASE_ID.equals(parsedBookmark.namedDatabaseId())) {
                        maxSystemDbTxId = Math.max(maxSystemDbTxId, parsedBookmark.txId());
                    } else {
                        if (userDbId == null) {
                            userDbId = parsedBookmark.namedDatabaseId();
                        } else if (!userDbId.equals(parsedBookmark.namedDatabaseId())) {
                            throw new InvalidBookmarkMixtureException(bookmarks);
                        }
                        maxUserDbTxId = Math.max(maxUserDbTxId, parsedBookmark.txId());
                    }
                }
            }
        }

        if (customBookmarkStrings.isEmpty()) {
            return buildBookmarks(maxSystemDbTxId, userDbId, maxUserDbTxId);
        }

        List<Bookmark> customBookmarks;
        try {
            customBookmarks = customBookmarkFormatParser.parse(customBookmarkStrings);
        } catch (Exception e) {
            throw new MalformedBookmarkException(
                    "Parsing of supplied bookmarks failed with message: " + e.getMessage(), e);
        }

        if (maxSystemDbTxId != ABSENT_BOOKMARK_ID) {
            customBookmarks.add(new BookmarkWithDatabaseId(maxSystemDbTxId, NAMED_SYSTEM_DATABASE_ID));
        }

        if (maxUserDbTxId != ABSENT_BOOKMARK_ID) {
            customBookmarks.add(new BookmarkWithDatabaseId(maxUserDbTxId, userDbId));
        }

        return customBookmarks;
    }

    private BookmarkWithDatabaseId parse(String bookmarkString) throws BookmarkParserException {
        var split = bookmarkString.split(":");
        if (split.length != 2) {
            throw new BookmarkFormatException(bookmarkString);
        }

        var databaseUuid = parseDatabaseId(split[0], bookmarkString);
        var txId = parseTxId(split[1], bookmarkString);

        var databaseId = databaseIdRepository
                .getById(DatabaseIdFactory.from(databaseUuid))
                .orElseThrow(() -> new UnknownDatabaseBookmarkException(databaseUuid));

        return new BookmarkWithDatabaseId(txId, databaseId);
    }

    private static String toBookmarkString(AnyValue bookmark) throws BookmarkParserException {
        if (bookmark instanceof TextValue bookmarkString) {
            return bookmarkString.stringValue();
        }

        throw new BookmarkFormatException(bookmark);
    }

    private static UUID parseDatabaseId(String uuid, String bookmark) throws BookmarkParserException {
        try {
            return UUID.fromString(uuid);
        } catch (IllegalArgumentException e) {
            throw new BookmarkFormatException(bookmark, format("Unable to parse database id: %s", uuid), e);
        }
    }

    private static long parseTxId(String txIdString, String bookmark) throws BookmarkParserException {
        try {
            return Long.parseLong(txIdString);
        } catch (NumberFormatException e) {
            throw new BookmarkFormatException(bookmark, format("Unable to parse transaction id: %s", txIdString), e);
        }
    }

    private static List<Bookmark> buildBookmarks(long maxSystemDbTxId, NamedDatabaseId userDbId, long maxUserDbTxId) {
        if (maxSystemDbTxId != ABSENT_BOOKMARK_ID && maxUserDbTxId != ABSENT_BOOKMARK_ID) {
            return List.of(
                    new BookmarkWithDatabaseId(maxSystemDbTxId, NAMED_SYSTEM_DATABASE_ID),
                    new BookmarkWithDatabaseId(maxUserDbTxId, userDbId));
        } else if (maxSystemDbTxId != ABSENT_BOOKMARK_ID) {
            return List.of(new BookmarkWithDatabaseId(maxSystemDbTxId, NAMED_SYSTEM_DATABASE_ID));
        } else if (maxUserDbTxId != ABSENT_BOOKMARK_ID) {
            return List.of(new BookmarkWithDatabaseId(maxUserDbTxId, userDbId));
        } else {
            return List.of();
        }
    }
}
