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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * This transaction bookmark manger is used when fabric bookmarks are used only for "Fabric" database.
 * In other words, non-fabric databases are not using fabric bookmarks.
 * <p>
 * The most specific thing about this transaction bookmark manger is that it handles mixture of fabric and non-fabric bookmarks
 * as it must handle System database bookmarks, which are not fabric ones.
 */
public class MixedModeBookmarkManager extends FabricOnlyBookmarkManager
{
    private final LocalGraphTransactionIdTracker localGraphTransactionIdTracker;

    public MixedModeBookmarkManager( LocalGraphTransactionIdTracker localGraphTransactionIdTracker )
    {
        super( localGraphTransactionIdTracker );
        this.localGraphTransactionIdTracker = localGraphTransactionIdTracker;
    }

    @Override
    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        var bookmarksByType = sortOutByType( bookmarks );

        processSystemDatabase( bookmarksByType );
        super.processSubmittedByClient( bookmarksByType.fabricBookmarks );
    }

    private BookmarksByType sortOutByType( List<Bookmark> bookmarks )
    {

        long systemDbTxId = -1;
        List<Bookmark> fabricBookmarks = new ArrayList<>( bookmarks.size() );

        for ( var bookmark : bookmarks )
        {
            if ( bookmark instanceof FabricBookmark )
            {
                fabricBookmarks.add( bookmark );
            }
            else
            {
                if ( !bookmark.databaseId().equals( NAMED_SYSTEM_DATABASE_ID ) )
                {
                    throw new FabricException( Status.Transaction.InvalidBookmarkMixture, "Bookmark for unexpected database encountered: " + bookmark );
                }

                systemDbTxId = Math.max( systemDbTxId, bookmark.txId() );
            }
        }

        return new BookmarksByType( systemDbTxId, fabricBookmarks );
    }

    private void processSystemDatabase( BookmarksByType bookmarksByType )
    {
        if ( bookmarksByType.systemDbTxId != -1 )
        {
            localGraphTransactionIdTracker.awaitSystemGraphUpToDate( bookmarksByType.systemDbTxId );
        }
    }

    private static class BookmarksByType
    {

        private final long systemDbTxId;
        private final List<Bookmark> fabricBookmarks;

        BookmarksByType( Long systemDbTxId, List<Bookmark> fabricBookmarks )
        {
            this.systemDbTxId = systemDbTxId;
            this.fabricBookmarks = fabricBookmarks;
        }
    }
}
