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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.CatchUpResponseAdaptor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Used client side for event handling of a store listing request
 */
public class PrepareStoreCopyResponseAdaptor extends CatchUpResponseAdaptor<PrepareStoreCopyResponse>
{
    private final StoreFileStreams storeFileStreams;
    private final Log log;
    private String destination;
    private int requiredAlignment;

    public PrepareStoreCopyResponseAdaptor( StoreFileStreams storeFileStreams, LogProvider logProvider )
    {
        this.storeFileStreams = storeFileStreams;
        log = logProvider.getLog( PrepareStoreCopyResponseAdaptor.class );
    }

    @Override
    public void onStoreListingResponse( CompletableFuture<PrepareStoreCopyResponse> signal, PrepareStoreCopyResponse response )
    {
        log.debug( "Complete download of file %s", destination );
        signal.complete( response );
    }

    @Override
    public void onFileHeader( CompletableFuture<PrepareStoreCopyResponse> requestOutcomeSignal, FileHeader fileHeader )
    {
        log.debug( "Received file header for file %s", fileHeader.fileName() );
        this.destination = fileHeader.fileName();
        this.requiredAlignment = fileHeader.requiredAlignment();
    }

    @Override
    public boolean onFileContent( CompletableFuture<PrepareStoreCopyResponse> signal, FileChunk fileChunk ) throws IOException
    {
        log.debug( "Received %b bytes for file %s", fileChunk.bytes(), destination );
        storeFileStreams.write( destination, requiredAlignment, fileChunk.bytes() );
        return fileChunk.isLast();
    }
}
