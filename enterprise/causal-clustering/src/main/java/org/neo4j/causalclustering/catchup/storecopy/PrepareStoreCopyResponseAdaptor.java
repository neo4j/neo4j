/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Used client side for event handling of a store listing request
 */
public class PrepareStoreCopyResponseAdaptor extends CatchUpResponseAdaptor<PrepareStoreCopyResponse>
{
    private final StoreFileStreamProvider storeFileStreamProvider;
    private final Log log;
    private StoreFileStream storeFileStream;

    PrepareStoreCopyResponseAdaptor( StoreFileStreamProvider storeFileStreamProvider, LogProvider logProvider )
    {
        this.storeFileStreamProvider = storeFileStreamProvider;
        log = logProvider.getLog( PrepareStoreCopyResponseAdaptor.class );
    }

    @Override
    public void onStoreListingResponse( CompletableFuture<PrepareStoreCopyResponse> signal, PrepareStoreCopyResponse response )
    {
        signal.complete( response );
    }

    @Override
    public void onFileHeader( CompletableFuture<PrepareStoreCopyResponse> requestOutcomeSignal, FileHeader fileHeader )
    {
        log.debug( "Received file header for file %s", fileHeader.fileName() );
        try
        {
            storeFileStream = storeFileStreamProvider.acquire( fileHeader.fileName(), fileHeader.requiredAlignment() );
            requestOutcomeSignal.whenComplete( ( storeCopyFinishedResponse, throwable ) ->
            {
                try
                {
                    storeFileStream.close();
                }
                catch ( Exception e )
                {
                    log.error( "Unable to close store file stream", e );
                }
            } );
        }
        catch ( IOException e )
        {
            requestOutcomeSignal.completeExceptionally( e );
        }
    }

    @Override
    public boolean onFileContent( CompletableFuture<PrepareStoreCopyResponse> signal, FileChunk fileChunk )
    {
        try
        {
            storeFileStream.write( fileChunk.bytes() );
        }
        catch ( IOException e )
        {
            signal.completeExceptionally( e );
        }
        return fileChunk.isLast();
    }
}
