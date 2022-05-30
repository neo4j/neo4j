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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmarkMixture;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;
import static org.neo4j.values.virtual.VirtualValues.list;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.bolt.protocol.common.bookmark.BookmarksParser;
import org.neo4j.bolt.protocol.common.message.result.ResponseHandler;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkFormatException;
import org.neo4j.bolt.protocol.error.bookmark.BookmarkParserException;
import org.neo4j.bolt.protocol.error.bookmark.InvalidBookmarkMixtureException;
import org.neo4j.bolt.protocol.error.bookmark.MalformedBookmarkException;
import org.neo4j.bolt.protocol.error.bookmark.UnknownDatabaseBookmarkException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.VirtualValues;

class BookmarksParserV40Test {
    private static final NamedDatabaseId dbId = from("alice", UUID.fromString("75a9d550-4a35-4bb9-a4b0-3e78a9b6958a"));
    private static final NamedDatabaseId dbId2 = from("bob", UUID.fromString("2796278e-42a0-43a1-a120-29959dc6a35b"));
    private static final NamedDatabaseId systemDbId = NAMED_SYSTEM_DATABASE_ID;

    private final DatabaseIdRepository idRepository = mock(DatabaseIdRepository.class);
    private final BookmarksParser parser = new BookmarksParserV40(idRepository, CustomBookmarkFormatParser.DEFAULT);

    @BeforeEach
    void setUp() {
        when(idRepository.getById(dbId.databaseId())).thenReturn(Optional.of(dbId));
        when(idRepository.getById(dbId2.databaseId())).thenReturn(Optional.of(dbId2));
        when(idRepository.getById(systemDbId.databaseId())).thenReturn(Optional.of(systemDbId));
    }

    @Test
    void shouldParseMultipleFormattedBookmarksContainingTransactionId() throws Exception {
        var txId1 = 1234;
        var txId2 = 12345;
        var metadata = list(bookmarkString(txId1, dbId), bookmarkString(txId2, dbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new BookmarkWithDatabaseId(txId2, dbId)), bookmarks);
    }

    @Test
    void shouldParseMultipleBookmarksContainingTransactionId() throws Exception {
        var bookmark1 = bookmarkString(1234, dbId);
        var bookmark2 = bookmarkString(12345, dbId);
        var metadata = list(bookmark1, bookmark2);

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new BookmarkWithDatabaseId(12345, dbId)), bookmarks);
    }

    @Test
    void shouldFailWhenParsingBadlyFormattedBookmark() {
        var bookmarkString = bookmarkString();
        var wrongBookmarkString = stringValue("neo4j:1234:v9:xt998");

        assertThatExceptionOfType(BookmarkFormatException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(bookmarkString, wrongBookmarkString)))
                .withMessage(
                        "Supplied bookmark(s) 'neo4j:1234:v9:xt998' does not conform to pattern {database_id}:{tx_id}.")
                .withNoCause()
                .satisfies(hasStatus(InvalidBookmark));
    }

    @Test
    void shouldFailWhenProvidingBookmarkInOldFormat() {
        var bookmarkString = stringValue("neo4j:bookmark:v1:tx10");

        assertThatExceptionOfType(BookmarkFormatException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(bookmarkString)))
                .withMessage(
                        "Supplied bookmark(s) 'neo4j:bookmark:v1:tx10' does not conform to pattern {database_id}:{tx_id}.")
                .withNoCause()
                .satisfies(hasStatus(InvalidBookmark));
    }

    @Test
    void shouldFailWhenMixingBookmarksFromDifferentDatabases() {
        var bookmarkString = bookmarkString(1234, dbId);
        var wrongBookmarkString = bookmarkString(4567, dbId2);

        assertThatExceptionOfType(InvalidBookmarkMixtureException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(bookmarkString, wrongBookmarkString)))
                .withMessage(
                        "Supplied bookmarks are from different databases. Bookmarks: List{String(\"75a9d550-4a35-4bb9-a4b0-3e78a9b6958a:1234\"), "
                                + "String(\"2796278e-42a0-43a1-a120-29959dc6a35b:4567\")}.")
                .withNoCause()
                .satisfies(hasStatus(InvalidBookmarkMixture));
    }

    @Test
    void shouldFailWhenBookmarkForUnknownDatabase() {
        var repository = mock(DatabaseIdRepository.class, RETURNS_DEFAULTS);
        var parser = new BookmarksParserV40(repository, CustomBookmarkFormatParser.DEFAULT);

        var bookmarkString = bookmarkString();

        assertThatExceptionOfType(UnknownDatabaseBookmarkException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(bookmarkString)))
                .withMessage("Supplied bookmark is for an unknown database: 2796278e-42a0-43a1-a120-29959dc6a35b.")
                .withNoCause()
                .satisfies(hasStatus(InvalidBookmark));
    }

    @Test
    void shouldReturnNoBookmarksWhenGivenBothSingleAndEmptyListAsMultiple() throws Exception {
        var bookmarks = parser.parseBookmarks(EMPTY_LIST);

        assertEquals(List.of(), bookmarks);
    }

    @Test
    void shouldReturnNullWhenNoBookmarks() throws Exception {
        assertEquals(List.of(), parser.parseBookmarks(VirtualValues.EMPTY_LIST));
    }

    @Test
    void shouldSkipNullsInMultipleBookmarks() throws Exception {
        var metadata = list(bookmarkString(3, dbId), bookmarkString(5, dbId), NO_VALUE, bookmarkString(17, dbId));
        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new BookmarkWithDatabaseId(17, dbId)), bookmarks);
    }

    @Test
    void shouldThrowWhenMultipleBookmarksIsNotAListOfStrings() {
        var metadata = list(bookmarkString(), longValue(42));

        assertThatExceptionOfType(BookmarkFormatException.class)
                .isThrownBy(() -> parser.parseBookmarks(metadata))
                .withMessage("Supplied bookmark(s) 'Long(42)' does not conform to pattern {database_id}:{tx_id}.")
                .withNoCause()
                .satisfies(ex -> assertThat(ex.status()).isEqualTo(InvalidBookmark));
    }

    @Test
    void shouldParseSingleSystemDbBookmark() throws Exception {
        var systemDbId = NAMED_SYSTEM_DATABASE_ID;
        var metadata = list(bookmarkString(42, systemDbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new BookmarkWithDatabaseId(42, systemDbId)), bookmarks);
    }

    @Test
    void shouldParseMultipleSystemDbBookmarks() throws Exception {
        var systemDbId = NAMED_SYSTEM_DATABASE_ID;
        var metadata =
                list(bookmarkString(42, systemDbId), bookmarkString(1, systemDbId), bookmarkString(39, systemDbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new BookmarkWithDatabaseId(42, systemDbId)), bookmarks);
    }

    @Test
    void shouldParseSingleSystemAndSingleUserDbBookmarks() throws Exception {
        var metadata = list(bookmarkString(33, systemDbId), bookmarkString(22, dbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(
                List.of(new BookmarkWithDatabaseId(33, systemDbId), new BookmarkWithDatabaseId(22, dbId)), bookmarks);
    }

    @Test
    void shouldParseMultipleSystemAndSingleUserDbBookmarks() throws Exception {
        var systemDbId = NAMED_SYSTEM_DATABASE_ID;
        var metadata = list(bookmarkString(33, systemDbId), bookmarkString(9, dbId), bookmarkString(44, systemDbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(
                List.of(new BookmarkWithDatabaseId(44, systemDbId), new BookmarkWithDatabaseId(9, dbId)), bookmarks);
    }

    @Test
    void shouldParseMultipleSystemAndMultipleUserDbBookmarks() throws Exception {
        var metadata = list(
                bookmarkString(12, systemDbId),
                bookmarkString(69, dbId),
                bookmarkString(83, systemDbId),
                bookmarkString(17, dbId));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(
                List.of(new BookmarkWithDatabaseId(83, systemDbId), new BookmarkWithDatabaseId(69, dbId)), bookmarks);
    }

    @Test
    void shouldErrorWhenDatabaseIdContainsInvalidUuid() {
        var wrongBookmarkString = stringValue("neo4j:1234");

        assertThatExceptionOfType(BookmarkFormatException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(wrongBookmarkString)))
                .withMessage(
                        "Supplied bookmark(s) 'neo4j:1234' does not conform to pattern {database_id}:{tx_id}: Unable to parse database id: neo4j.")
                .withCauseInstanceOf(IllegalArgumentException.class)
                .satisfies(ex -> assertThat(ex.status()).isEqualTo(InvalidBookmark));
    }

    @Test
    void shouldErrorWhenDatabaseIdContainsInvalidTxId() throws Exception {
        var wrongBookmarkString =
                stringValue(String.format("%s:neo4j", dbId.databaseId().uuid().toString()));

        assertThatExceptionOfType(BookmarkFormatException.class)
                .isThrownBy(() -> parser.parseBookmarks(list(wrongBookmarkString)))
                .withMessage(
                        "Supplied bookmark(s) '75a9d550-4a35-4bb9-a4b0-3e78a9b6958a:neo4j' does not conform to pattern {database_id}:{tx_id}: "
                                + "Unable to parse transaction id: neo4j.")
                .withCauseInstanceOf(NumberFormatException.class)
                .satisfies(ex -> assertThat(ex.status()).isEqualTo(InvalidBookmark));
    }

    @Test
    void shouldParseCustomBookmarks() throws BookmarkParserException {
        var parser = new BookmarksParserV40(mock(DatabaseIdRepository.class), new CustomParser());

        var metadata = list(customBookmarkString("text1"), customBookmarkString("text2"));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(List.of(new CustomBookmark("text1"), new CustomBookmark("text2")), bookmarks);
    }

    @Test
    void shouldParseCustomBookmarksMixUp() throws BookmarkParserException {
        var parser = new BookmarksParserV40(idRepository, new CustomParser());

        var metadata = list(
                customBookmarkString("text1"),
                bookmarkString(1234, systemDbId),
                bookmarkString(4321, dbId),
                customBookmarkString("text2"));

        var bookmarks = parser.parseBookmarks(metadata);

        assertEquals(
                List.of(
                        new CustomBookmark("text1"),
                        new CustomBookmark("text2"),
                        new BookmarkWithDatabaseId(1234, systemDbId),
                        new BookmarkWithDatabaseId(4321, dbId)),
                bookmarks);
    }

    @Test
    void shouldThrowBoltExceptionWhenCustomBookmarksParsingFails() {
        var cause = new IllegalArgumentException("This bookmark is just wrong");
        var parser = new BookmarksParserV40(mock(DatabaseIdRepository.class), new CustomBookmarkFormatParser() {
            @Override
            public boolean isCustomBookmark(String string) {
                return true;
            }

            @Override
            public List<Bookmark> parse(List<String> customBookmarks) {
                throw cause;
            }
        });

        var metadata = list(stringValue(""));

        assertThatExceptionOfType(MalformedBookmarkException.class)
                .isThrownBy(() -> parser.parseBookmarks(metadata))
                .withMessage("Parsing of supplied bookmarks failed with message: This bookmark is just wrong")
                .withCause(cause)
                .satisfies(ex -> assertThat(ex.status()).isEqualTo(InvalidBookmark));
    }

    private static TextValue bookmarkString(long txId, NamedDatabaseId namedDatabaseId) {
        return stringValue(new BookmarkWithDatabaseId(txId, namedDatabaseId).toString());
    }

    private static TextValue customBookmarkString(String text) {
        return stringValue("custom:" + text);
    }

    /**
     * Create a random bookmark
     */
    private static TextValue bookmarkString() {
        var dbId = from("peter", dbId2.databaseId().uuid());
        return bookmarkString(1234, dbId);
    }

    public static <E extends Throwable> Consumer<E> hasStatus(Status status) {
        return ex -> assertSoftly(soft -> {
            soft.assertThat(ex).as("implements Status.HasStatus").isInstanceOf(Status.HasStatus.class);

            soft.assertThat(((Status.HasStatus) ex).status())
                    .as("returns status %s", status)
                    .isEqualTo(status);
        });
    }

    private static class CustomParser implements CustomBookmarkFormatParser {

        @Override
        public boolean isCustomBookmark(String bookmark) {
            return bookmark.startsWith("custom:");
        }

        @Override
        public List<Bookmark> parse(List<String> customBookmarks) {
            return customBookmarks.stream()
                    .map(bookmark -> bookmark.substring("custom:".length()))
                    .map(CustomBookmark::new)
                    .collect(Collectors.toList());
        }
    }

    private static class CustomBookmark implements Bookmark {

        private final String text;

        CustomBookmark(String text) {
            this.text = text;
        }

        @Override
        public long txId() {
            throw new IllegalStateException("ID requested on a custom bookmark");
        }

        @Override
        public NamedDatabaseId databaseId() {
            throw new IllegalStateException("Database ID requested on a custom bookmark");
        }

        @Override
        public void attachTo(ResponseHandler state) {}

        public String getText() {
            return text;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomBookmark that = (CustomBookmark) o;
            return Objects.equals(text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text);
        }

        @Override
        public String toString() {
            return "CustomBookmark{" + "text='" + text + '\'' + '}';
        }
    }
}
