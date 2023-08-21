/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.bookmark;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.executor.Location;

public class TransactionBookmarkManagerImpl implements TransactionBookmarkManager {
    private final QueryRouterBookmark submittedBookmark;

    // must be taken when updating the final bookmark
    private final Object finalBookmarkLock = new Object();
    private volatile QueryRouterBookmark finalBookmark;

    public TransactionBookmarkManagerImpl(List<QueryRouterBookmark> bookmarksSubmittedByClient) {
        this.submittedBookmark = merge(bookmarksSubmittedByClient);
        this.finalBookmark = new QueryRouterBookmark(
                new ArrayList<>(submittedBookmark.internalGraphStates()),
                new ArrayList<>(submittedBookmark.externalGraphStates()));
    }

    @Override
    public Optional<LocalBookmark> getBookmarkForLocal(Location.Local location) {
        return submittedBookmark.internalGraphStates().stream()
                .filter(egs -> egs.graphUuid().equals(location.getUuid()))
                .map(QueryRouterBookmark.InternalGraphState::transactionId)
                .map(LocalBookmark::new)
                .findAny();
    }

    @Override
    public Optional<LocalBookmark> getBookmarkForLocalSystemDatabase() {
        return submittedBookmark.internalGraphStates().stream()
                .filter(internalGraphState -> internalGraphState
                        .graphUuid()
                        .equals(NAMED_SYSTEM_DATABASE_ID.databaseId().uuid()))
                .map(internalGraphState -> new LocalBookmark(internalGraphState.transactionId()))
                .findFirst();
    }

    @Override
    public List<RemoteBookmark> getBookmarksForRemote(Location.Remote location) {
        if (location instanceof Location.Remote.External) {
            return submittedBookmark.externalGraphStates().stream()
                    .filter(egs -> egs.graphUuid().equals(location.getUuid()))
                    .map(QueryRouterBookmark.ExternalGraphState::bookmarks)
                    .findAny()
                    .orElse(List.of());
        }

        // The inter-cluster remote needs the same bookmark data that was submitted to the this DBMS
        var serializedBookmark = BookmarkFormat.serialize(submittedBookmark);
        return List.of(new RemoteBookmark(serializedBookmark));
    }

    @Override
    public void remoteTransactionCommitted(Location.Remote location, RemoteBookmark bookmark) {
        if (bookmark == null) {
            return;
        }

        synchronized (finalBookmarkLock) {
            if (location instanceof Location.Remote.External) {
                var externalGraphState =
                        new QueryRouterBookmark.ExternalGraphState(location.getUuid(), List.of(bookmark));
                var bookmarkUpdate = new QueryRouterBookmark(List.of(), List.of(externalGraphState));
                finalBookmark = merge(List.of(finalBookmark, bookmarkUpdate));
            } else {
                var parsedBookmark = BookmarkFormat.parse(bookmark.serializedState());
                finalBookmark = merge(List.of(finalBookmark, parsedBookmark));
            }
        }
    }

    @Override
    public void localTransactionCommitted(Location.Local location, LocalBookmark bookmark) {
        synchronized (finalBookmarkLock) {
            var internalGraphState =
                    new QueryRouterBookmark.InternalGraphState(location.getUuid(), bookmark.transactionId());
            var bookmarkUpdate = new QueryRouterBookmark(List.of(internalGraphState), List.of());
            finalBookmark = merge(List.of(finalBookmark, bookmarkUpdate));
        }
    }

    @Override
    public QueryRouterBookmark constructFinalBookmark() {
        return finalBookmark;
    }

    private static QueryRouterBookmark merge(List<QueryRouterBookmark> queryRouterBookmarks) {
        List<QueryRouterBookmark.InternalGraphState> mergedInternalGraphStates =
                mergeInternalGraphStates(queryRouterBookmarks);
        List<QueryRouterBookmark.ExternalGraphState> mergedExternalGraphStates =
                mergeExternalGraphStates(queryRouterBookmarks);

        return new QueryRouterBookmark(mergedInternalGraphStates, mergedExternalGraphStates);
    }

    private static List<QueryRouterBookmark.InternalGraphState> mergeInternalGraphStates(
            List<QueryRouterBookmark> queryRouterBookmarks) {
        Map<UUID, Long> internalGraphTxIds = new HashMap<>();

        queryRouterBookmarks.stream()
                .flatMap(fabricBookmark -> fabricBookmark.internalGraphStates().stream())
                .forEach(internalGraphState -> internalGraphTxIds.merge(
                        internalGraphState.graphUuid(), internalGraphState.transactionId(), Math::max));

        return internalGraphTxIds.entrySet().stream()
                .map(entry -> new QueryRouterBookmark.InternalGraphState(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    private static List<QueryRouterBookmark.ExternalGraphState> mergeExternalGraphStates(
            List<QueryRouterBookmark> queryRouterBookmarks) {
        Map<UUID, List<RemoteBookmark>> externalGraphStates = new HashMap<>();

        queryRouterBookmarks.stream()
                .flatMap(fabricBookmark -> fabricBookmark.externalGraphStates().stream())
                .forEach(externalGraphState -> externalGraphStates
                        .computeIfAbsent(externalGraphState.graphUuid(), key -> new ArrayList<>())
                        .addAll(externalGraphState.bookmarks()));

        return externalGraphStates.entrySet().stream()
                .map(entry -> new QueryRouterBookmark.ExternalGraphState(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }
}
