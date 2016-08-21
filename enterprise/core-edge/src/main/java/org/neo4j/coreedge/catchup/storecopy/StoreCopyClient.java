/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.storecopy;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import org.neo4j.coreedge.catchup.CatchUpClient;
import org.neo4j.coreedge.catchup.CatchUpClientException;
import org.neo4j.coreedge.catchup.CatchUpResponseAdaptor;
import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.identity.StoreId;

import static java.util.concurrent.TimeUnit.SECONDS;

public class StoreCopyClient
{
    private final CatchUpClient catchUpClient;

    public StoreCopyClient( CatchUpClient catchUpClient )
    {
        this.catchUpClient = catchUpClient;
    }

    long copyStoreFiles( MemberId from, StoreFileStreams storeFileStreams ) throws StoreCopyFailedException
    {
        try
        {
            return catchUpClient.makeBlockingRequest( from, new GetStoreRequest(), 30, SECONDS,
                    new CatchUpResponseAdaptor<Long>()
                    {
                        private long expectedBytes = 0;
                        private String destination;

                        @Override
                        public void onFileHeader( CompletableFuture<Long> requestOutcomeSignal, FileHeader fileHeader )
                        {
                            this.expectedBytes = fileHeader.fileLength();
                            this.destination = fileHeader.fileName();
                        }

                        @Override
                        public boolean onFileContent( CompletableFuture<Long> signal, FileContent fileContent )
                                throws IOException
                        {
                            try ( FileContent content = fileContent;
                                  OutputStream outputStream = storeFileStreams.createStream( destination ) )
                            {
                                expectedBytes -= content.writeTo( outputStream );
                            }

                            return expectedBytes <= 0;
                        }

                        @Override
                        public void onFileStreamingComplete( CompletableFuture<Long> signal,
                                                             StoreCopyFinishedResponse response )
                        {
                            signal.complete( response.lastCommittedTxBeforeStoreCopy() );
                        }
                    } );
        }
        catch ( CatchUpClientException | NoKnownAddressesException e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    StoreId fetchStoreId( MemberId from ) throws StoreIdDownloadFailedException
    {
        try
        {
            return catchUpClient.makeBlockingRequest( from, new GetStoreIdRequest(), 30, SECONDS,
                    new CatchUpResponseAdaptor<StoreId>()
                    {
                        @Override
                        public void onGetStoreIdResponse( CompletableFuture<StoreId> signal,
                                                          GetStoreIdResponse response )
                        {
                            signal.complete( response.storeId() );
                        }
                    } );
        }
        catch ( CatchUpClientException | NoKnownAddressesException e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }
}
