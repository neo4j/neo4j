/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.logging.Log;

import static java.lang.String.format;

public abstract class StoreCopyResponseAdaptors<T> extends CatchUpResponseAdaptor<T>
{
    static StoreCopyResponseAdaptors<StoreCopyFinishedResponse> filesCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        return new StoreFilesCopyResponseAdaptors( storeFileStreamProvider, log );
    }

    static StoreCopyResponseAdaptors<PrepareStoreCopyResponse> prepareStoreCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        return new PrepareStoreCopyResponseAdaptors( storeFileStreamProvider, log );
    }

    private final StoreFileStreamProvider storeFileStreamProvider;
    private final Log log;
    private StoreFileStream storeFileStream;

    private StoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        this.storeFileStreamProvider = storeFileStreamProvider;
        this.log = log;
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
        PrepareStoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
        {
            super( storeFileStreamProvider, log );
        }

        @Override
        public void onStoreListingResponse( CompletableFuture<PrepareStoreCopyResponse> signal, PrepareStoreCopyResponse response )
        {
            signal.complete( response );
        }
    }

    private static class StoreFilesCopyResponseAdaptors extends StoreCopyResponseAdaptors<StoreCopyFinishedResponse>
    {
        StoreFilesCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
        {
            super( storeFileStreamProvider, log );
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
                log.error( format( "Unable to close store file stream for file '%s'", fileName ), e );
            }
        }
    }
}
