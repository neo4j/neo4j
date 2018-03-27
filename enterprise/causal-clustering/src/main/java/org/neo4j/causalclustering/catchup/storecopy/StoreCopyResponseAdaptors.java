/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.causalclustering.messaging.EventHandler;

import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public abstract class StoreCopyResponseAdaptors<T> extends CatchUpResponseAdaptor<T>
{
    static StoreCopyResponseAdaptors<StoreCopyFinishedResponse> filesCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider, EventHandler eventHandler )
    {
        return new StoreFilesCopyResponseAdaptors( storeFileStreamProvider, eventHandler );
    }

    static StoreCopyResponseAdaptors<PrepareStoreCopyResponse> prepareStoreCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider,
            EventHandler eventHandler )
    {
        return new PrepareStoreCopyResponseAdaptors( storeFileStreamProvider, eventHandler );
    }

    private final StoreFileStreamProvider storeFileStreamProvider;
    private final EventHandler eventHandler;
    private StoreFileStream storeFileStream;

    private StoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, EventHandler eventHandler )
    {
        this.storeFileStreamProvider = storeFileStreamProvider;
        this.eventHandler = eventHandler;
    }

    /**
     * Files will be sent in order but multiple files may be sent during one response.
     *
     * @param requestOutcomeSignal signal
     * @param fileHeader header for most resent file being sent
     */
    @Override
    public void onFileHeader( CompletableFuture<T> requestOutcomeSignal, FileHeader fileHeader )
    {
        try
        {
            final StoreFileStream fileStream = storeFileStreamProvider.acquire( fileHeader.fileName(), fileHeader.requiredAlignment() );
            // Make sure that each stream closes on complete but only the latest is written to
            requestOutcomeSignal.whenComplete( new CloseFileStreamOnComplete<>( fileStream, fileHeader.fileName() ) );
            this.storeFileStream = fileStream;
            eventHandler.on( EventHandler.EventState.Info, "Receiving file", param( "File", fileHeader.fileName() ) );
        }
        catch ( Exception e )
        {
            requestOutcomeSignal.completeExceptionally( e );
        }
    }

    @Override
    public boolean onFileContent( CompletableFuture<T> signal, FileChunk fileChunk )
    {
        try
        {
            storeFileStream.write( fileChunk.bytes() );
        }
        catch ( Exception e )
        {
            signal.completeExceptionally( e );
        }
        return fileChunk.isLast();
    }

    private static class PrepareStoreCopyResponseAdaptors extends StoreCopyResponseAdaptors<PrepareStoreCopyResponse>
    {
        PrepareStoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, EventHandler eventHandler )
        {
            super( storeFileStreamProvider, eventHandler );
        }

        @Override
        public void onStoreListingResponse( CompletableFuture<PrepareStoreCopyResponse> signal, PrepareStoreCopyResponse response )
        {
            signal.complete( response );
        }
    }

    private static class StoreFilesCopyResponseAdaptors extends StoreCopyResponseAdaptors<StoreCopyFinishedResponse>
    {
        StoreFilesCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, EventHandler eventHandler )
        {
            super( storeFileStreamProvider, eventHandler );
        }

        @Override
        public void onFileStreamingComplete( CompletableFuture<StoreCopyFinishedResponse> signal, StoreCopyFinishedResponse response )
        {
            signal.complete( response );
        }
    }

    private class CloseFileStreamOnComplete<RESPONSE> implements BiConsumer<RESPONSE,Throwable>
    {
        private final StoreFileStream fileStream;
        private String fileName;

        private CloseFileStreamOnComplete( StoreFileStream fileStream, String fileName )
        {
            this.fileStream = fileStream;
            this.fileName = fileName;
        }

        @Override
        public void accept( RESPONSE response, Throwable throwable )
        {
            try
            {
                fileStream.close();
            }
            catch ( Exception e )
            {
                eventHandler.on( EventHandler.EventState.Error, "Unable to close store file stream.", e, param( "File", fileName ) );
            }
        }
    }
}
