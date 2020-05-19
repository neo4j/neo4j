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
import java.util.stream.Collectors;

import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.api.exceptions.Status;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class TransactionBookmarkManagerImpl implements TransactionBookmarkManager
{
    private final FabricBookmarkParser fabricBookmarkParser = new FabricBookmarkParser();
    private final LocalGraphTransactionIdTracker transactionIdTracker;
    private final boolean multiGraphEverywhere;

    // must be taken when updating the final bookmark
    private final Object finalBookmarkLock = new Object();

    private volatile FabricBookmark submittedBookmark;
    private volatile FabricBookmark finalBookmark;

    public TransactionBookmarkManagerImpl( LocalGraphTransactionIdTracker transactionIdTracker, boolean multiGraphEverywhere )
    {
        this.transactionIdTracker = transactionIdTracker;
        this.multiGraphEverywhere = multiGraphEverywhere;
    }

    @Override
    public void processSubmittedByClient( List<Bookmark> bookmarks )
    {
        var fabricBookmarks = convert( bookmarks );
        this.submittedBookmark = FabricBookmark.merge( fabricBookmarks );
        this.finalBookmark = new FabricBookmark(
                new ArrayList<>( submittedBookmark.getInternalGraphStates() ),
                new ArrayList<>( submittedBookmark.getExternalGraphStates() )
        );
        // regardless of what we do, System graph must be always up to date
        awaitSystemGraphUpToDate();
    }

    private List<FabricBookmark> convert( List<Bookmark> bookmarks )
    {
        return bookmarks.stream().map( bookmark ->
        {
            if ( bookmark instanceof FabricBookmark )
            {
                return (FabricBookmark) bookmark;
            }

            // Getting non-fabric bookmarks when 'multi graph everywhere' is enabled
            // can only happen during rolling upgrade.
            // Let's be nice and convert the bookmarks into Fabric ones in this case.
            // If 'multi graph everywhere' is not enabled, only System DB bookmarks
            // can be mixed with target database bookmarks. That is not a Fabric restriction,
            // but how non-fabric bookmarks work.
            if ( !multiGraphEverywhere && !bookmark.databaseId().equals( NAMED_SYSTEM_DATABASE_ID ) )
            {
                throw new FabricException( Status.Transaction.InvalidBookmarkMixture, "Bookmark for unexpected database encountered: " + bookmark );
            }

            return convertNonFabricBookmark( bookmark );
        } ).collect( Collectors.toList() );
    }

    private FabricBookmark convertNonFabricBookmark( Bookmark bookmark )
    {
        var databaseUuid = bookmark.databaseId().databaseId().uuid();
        var internalGraphState = new FabricBookmark.InternalGraphState( databaseUuid, bookmark.txId() );
        return new FabricBookmark( List.of( internalGraphState ), List.of() );
    }

    private void awaitSystemGraphUpToDate()
    {
        var graphUuid2TxIdMapping = submittedBookmark.getInternalGraphStates().stream()
                .collect( Collectors.toMap( FabricBookmark.InternalGraphState::getGraphUuid, FabricBookmark.InternalGraphState::getTransactionId) );
        transactionIdTracker.awaitSystemGraphUpToDate( graphUuid2TxIdMapping );
    }

    @Override
    public List<RemoteBookmark> getBookmarksForRemote( Location.Remote location )
    {
        if ( location instanceof Location.Remote.External )
        {
            return submittedBookmark.getExternalGraphStates().stream()
                    .filter( egs -> egs.getGraphUuid().equals( location.getUuid() ) )
                    .map( FabricBookmark.ExternalGraphState::getBookmarks )
                    .findAny()
                    .orElse( List.of() );
        }

        // The inter-cluster remote needs the same bookmark data that was submitted to the this DBMS
        return List.of( new RemoteBookmark( submittedBookmark.serialize() ) );
    }

    @Override
    public void remoteTransactionCommitted( Location.Remote location, RemoteBookmark bookmark )
    {
        if ( bookmark == null )
        {
            return;
        }

        synchronized ( finalBookmarkLock )
        {
            if ( location instanceof Location.Remote.External )
            {
                var externalGraphState = new FabricBookmark.ExternalGraphState( location.getUuid(), List.of( bookmark ) );
                var bookmarkUpdate = new FabricBookmark( List.of(), List.of( externalGraphState ) );
                finalBookmark = FabricBookmark.merge( List.of( finalBookmark, bookmarkUpdate ) );
            }
            else
            {
                var fabricBookmark = fabricBookmarkParser.parse( bookmark.getSerialisedState() );
                finalBookmark = FabricBookmark.merge( List.of( finalBookmark, fabricBookmark ) );
            }
        }
    }

    @Override
    public void awaitUpToDate( Location.Local location )
    {
        submittedBookmark.getInternalGraphStates().stream()
                .filter( egs -> egs.getGraphUuid().equals( location.getUuid() ) )
                .map( FabricBookmark.InternalGraphState::getTransactionId )
                .findAny()
                .ifPresent( transactionId -> transactionIdTracker.awaitGraphUpToDate( location, transactionId ) );
    }

    @Override
    public void localTransactionCommitted( Location.Local location )
    {
        synchronized ( finalBookmarkLock )
        {
            long transactionId = transactionIdTracker.getTransactionId( location );
            var internalGraphState = new FabricBookmark.InternalGraphState( location.getUuid(), transactionId );
            var bookmarkUpdate = new FabricBookmark( List.of( internalGraphState ), List.of() );
            finalBookmark = FabricBookmark.merge( List.of( finalBookmark, bookmarkUpdate ) );
        }
    }

    @Override
    public FabricBookmark constructFinalBookmark()
    {
        return finalBookmark;
    }
}
