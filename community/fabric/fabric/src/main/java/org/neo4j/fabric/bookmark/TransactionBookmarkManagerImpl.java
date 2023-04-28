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
package org.neo4j.fabric.bookmark;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.neo4j.bolt.protocol.common.bookmark.Bookmark;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.api.exceptions.Status;

public class TransactionBookmarkManagerImpl implements TransactionBookmarkManager {
    private final FabricBookmarkParser fabricBookmarkParser = new FabricBookmarkParser();
    private final FabricBookmark submittedBookmark;

    // must be taken when updating the final bookmark
    private final Object finalBookmarkLock = new Object();
    private volatile FabricBookmark finalBookmark;

    public TransactionBookmarkManagerImpl(List<Bookmark> bookmarksSubmittedByClient) {
        var fabricBookmarks = convert(bookmarksSubmittedByClient);
        this.submittedBookmark = FabricBookmark.merge(fabricBookmarks);
        this.finalBookmark = new FabricBookmark(
                new ArrayList<>(submittedBookmark.getInternalGraphStates()),
                new ArrayList<>(submittedBookmark.getExternalGraphStates()));
    }

    @Override
    public Optional<LocalBookmark> getBookmarkForLocal(Location.Local location) {
        return submittedBookmark.getInternalGraphStates().stream()
                .filter(egs -> egs.graphUuid().equals(location.getUuid()))
                .map(FabricBookmark.InternalGraphState::transactionId)
                .map(LocalBookmark::new)
                .findAny();
    }

    @Override
    public Optional<LocalBookmark> getBookmarkForLocalSystemDatabase() {
        return submittedBookmark.getInternalGraphStates().stream()
                .filter(internalGraphState -> internalGraphState
                        .graphUuid()
                        .equals(NAMED_SYSTEM_DATABASE_ID.databaseId().uuid()))
                .map(internalGraphState -> new LocalBookmark(internalGraphState.transactionId()))
                .findFirst();
    }

    private List<FabricBookmark> convert(List<Bookmark> bookmarks) {
        return bookmarks.stream()
                .map(bookmark -> {
                    if (bookmark instanceof FabricBookmark) {
                        return (FabricBookmark) bookmark;
                    }

                    throw new FabricException(
                            Status.Transaction.InvalidBookmark,
                            "Bookmark for unexpected database encountered: " + bookmark);
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<RemoteBookmark> getBookmarksForRemote(Location.Remote location) {
        if (location instanceof Location.Remote.External) {
            return submittedBookmark.getExternalGraphStates().stream()
                    .filter(egs -> egs.graphUuid().equals(location.getUuid()))
                    .map(FabricBookmark.ExternalGraphState::bookmarks)
                    .findAny()
                    .orElse(List.of());
        }

        // The inter-cluster remote needs the same bookmark data that was submitted to the this DBMS
        return List.of(new RemoteBookmark(submittedBookmark.serialize()));
    }

    @Override
    public void remoteTransactionCommitted(Location.Remote location, RemoteBookmark bookmark) {
        if (bookmark == null) {
            return;
        }

        synchronized (finalBookmarkLock) {
            if (location instanceof Location.Remote.External) {
                var externalGraphState = new FabricBookmark.ExternalGraphState(location.getUuid(), List.of(bookmark));
                var bookmarkUpdate = new FabricBookmark(List.of(), List.of(externalGraphState));
                finalBookmark = FabricBookmark.merge(List.of(finalBookmark, bookmarkUpdate));
            } else {
                var fabricBookmark = fabricBookmarkParser.parse(bookmark.serializedState());
                finalBookmark = FabricBookmark.merge(List.of(finalBookmark, fabricBookmark));
            }
        }
    }

    @Override
    public void localTransactionCommitted(Location.Local location, LocalBookmark bookmark) {
        synchronized (finalBookmarkLock) {
            var internalGraphState =
                    new FabricBookmark.InternalGraphState(location.getUuid(), bookmark.transactionId());
            var bookmarkUpdate = new FabricBookmark(List.of(internalGraphState), List.of());
            finalBookmark = FabricBookmark.merge(List.of(finalBookmark, bookmarkUpdate));
        }
    }

    @Override
    public FabricBookmark constructFinalBookmark() {
        return finalBookmark;
    }
}
