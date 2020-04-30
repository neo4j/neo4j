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
package org.neo4j.fabric.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.neo4j.bolt.dbapi.BookmarkMetadata;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.fabric.bookmark.BookmarkStateSerializer;
import org.neo4j.fabric.bookmark.RemoteBookmark;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.neo4j.values.storable.Values.utf8Value;

public class FabricBookmark extends BookmarkMetadata implements Bookmark
{
    public static final String PREFIX = "FB:";

    private final List<InternalGraphState> internalGraphStates;
    private final List<ExternalGraphState> externalGraphStates;

    public FabricBookmark( List<InternalGraphState> internalGraphStates, List<ExternalGraphState> externalGraphStates )
    {
        super( -1, null );

        this.internalGraphStates = internalGraphStates;
        this.externalGraphStates = externalGraphStates;
    }

    public List<InternalGraphState> getInternalGraphStates()
    {
        return internalGraphStates;
    }

    public List<ExternalGraphState> getExternalGraphStates()
    {
        return externalGraphStates;
    }

    @Override
    public long txId()
    {
        return getTxId();
    }

    @Override
    public NamedDatabaseId databaseId()
    {
        return getNamedDatabaseId();
    }

    @Override
    public void attachTo( BoltResponseHandler state )
    {
        state.onMetadata( BOOKMARK_KEY, utf8Value( serialize() ) );
    }

    @Override
    public Bookmark toBookmark( BiFunction<Long,NamedDatabaseId, Bookmark> defaultBookmarkFormat )
    {
        return this;
    }

    public String serialize()
    {
        String serializedState = BookmarkStateSerializer.serialize( this );
        return PREFIX + serializedState;
    }

    public static FabricBookmark merge( List<FabricBookmark> fabricBookmarks )
    {
        List<InternalGraphState> mergedInternalGraphStates = mergeInternalGraphStates( fabricBookmarks );
        List<ExternalGraphState> mergedExternalGraphStates = mergeExternalGraphStates( fabricBookmarks );

        return new FabricBookmark( mergedInternalGraphStates, mergedExternalGraphStates );
    }

    private static List<InternalGraphState> mergeInternalGraphStates( List<FabricBookmark> fabricBookmarks  )
    {
        Map<UUID,Long> internalGraphTxIds = new HashMap<>();

        fabricBookmarks.stream()
                .flatMap( fabricBookmark -> fabricBookmark.getInternalGraphStates().stream() )
                .forEach( internalGraphState -> internalGraphTxIds.merge( internalGraphState.getGraphUuid(),
                        internalGraphState.getTransactionId(),
                        Math::max )
                );

        return internalGraphTxIds.entrySet().stream()
                .map( entry -> new InternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());
    }

    private static List<ExternalGraphState> mergeExternalGraphStates( List<FabricBookmark> fabricBookmarks  )
    {
        Map<UUID,List<RemoteBookmark>> externalGraphStates = new HashMap<>();

        fabricBookmarks.stream()
                .flatMap( fabricBookmark -> fabricBookmark.getExternalGraphStates().stream() )
                .forEach( externalGraphState -> externalGraphStates.computeIfAbsent( externalGraphState.getGraphUuid(), key -> new ArrayList<>() )
                        .addAll( externalGraphState.getBookmarks() )
                );

        return externalGraphStates.entrySet().stream()
                .map( entry -> new ExternalGraphState( entry.getKey(), entry.getValue() ) )
                .collect( Collectors.toList());
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FabricBookmark that = (FabricBookmark) o;
        return internalGraphStates.equals( that.internalGraphStates ) && externalGraphStates.equals( that.externalGraphStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( internalGraphStates, externalGraphStates );
    }

    @Override
    public String toString()
    {
        return "FabricBookmark{" + "internalGraphStates=" + internalGraphStates + ", externalGraphStates=" + externalGraphStates + '}';
    }

    /**
     * State of a graph that is located in current DBMS.
     */
    public static class InternalGraphState
    {
        private final UUID graphUuid;
        private final long transactionId;

        public InternalGraphState( UUID graphUuid, long transactionId )
        {
            this.graphUuid = graphUuid;
            this.transactionId = transactionId;
        }

        public UUID getGraphUuid()
        {
            return graphUuid;
        }

        public long getTransactionId()
        {
            return transactionId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            InternalGraphState that = (InternalGraphState) o;
            return transactionId == that.transactionId && graphUuid.equals( that.graphUuid );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( graphUuid, transactionId );
        }

        @Override
        public String toString()
        {
            return "InternalGraphState{" + "graphUuid=" + graphUuid + ", transactionId=" + transactionId + '}';
        }
    }

    /**
     * State of a graph that is located in another DBMS.
     */
    public static class ExternalGraphState
    {
        private final UUID graphUuid;
        private final List<RemoteBookmark> bookmarks;

        public ExternalGraphState( UUID graphUuid, List<RemoteBookmark> bookmarks )
        {
            this.graphUuid = graphUuid;
            this.bookmarks = bookmarks;
        }

        public UUID getGraphUuid()
        {
            return graphUuid;
        }

        public List<RemoteBookmark> getBookmarks()
        {
            return bookmarks;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            ExternalGraphState that = (ExternalGraphState) o;
            return graphUuid.equals( that.graphUuid ) && bookmarks.equals( that.bookmarks );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( graphUuid, bookmarks );
        }

        @Override
        public String toString()
        {
            return "ExternalGraphState{" + "graphUuid=" + graphUuid + ", bookmarks=" + bookmarks + '}';
        }
    }
}
