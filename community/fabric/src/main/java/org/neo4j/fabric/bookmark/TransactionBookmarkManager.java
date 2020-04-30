/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.List;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.executor.Location;

/**
 * Manges bookmarks for a single transaction.
 * <p>
 * An implementation MUST NOT be shared across transactions.
 */
public interface TransactionBookmarkManager
{
    void processSubmittedByClient( List<Bookmark> bookmarks );

    /**
     * Returns bookmarks that should be sent to a remote when opening a transaction there.
     */
    List<RemoteBookmark> getBookmarksForRemote( Location.Remote location );

    /**
     * Handle a bookmark received from a remote after a transaction has been committed there.
     */
    void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark );

    /**
     * Will wait until the local graph in a state required by bookmarks submitted in {@link #processSubmittedByClient(List)}.
     */
    void awaitUpToDate( Location.Local location );

    /**
     * Notifies the manager that a local transaction has been committed.
     */
    void localTransactionCommitted( Location.Local local );

    /**
     * Constructs a bookmark that will hold the information collected by this bookmark manager.
     */
    FabricBookmark constructFinalBookmark();
}
