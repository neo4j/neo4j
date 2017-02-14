/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup;

import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

@SuppressWarnings("unchecked")
class TrackingResponseHandler implements CatchUpResponseHandler
{
    private CatchUpResponseCallback delegate;
    private CompletableFuture<?> requestOutcomeSignal = new CompletableFuture<>();
    private final Clock clock;
    private Long lastResponseTime;

    TrackingResponseHandler( CatchUpResponseCallback delegate, Clock clock )
    {
        this.delegate = delegate;
        this.clock = clock;
    }

    void setResponseHandler( CatchUpResponseCallback responseHandler, CompletableFuture<?>
            requestOutcomeSignal )
    {
        this.delegate = responseHandler;
        this.requestOutcomeSignal = requestOutcomeSignal;
    }

    @Override
    public void onFileHeader( FileHeader fileHeader )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onFileHeader( requestOutcomeSignal, fileHeader );
        }
    }

    @Override
    public boolean onFileContent( FileChunk fileChunk ) throws IOException
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            return delegate.onFileContent( requestOutcomeSignal, fileChunk );
        }
        // true means stop
        return true;
    }

    @Override
    public void onFileStreamingComplete( StoreCopyFinishedResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onFileStreamingComplete( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onTxPullResponse( TxPullResponse tx )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onTxPullResponse( requestOutcomeSignal, tx );
        }
    }

    @Override
    public void onTxStreamFinishedResponse( TxStreamFinishedResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onTxStreamFinishedResponse( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onGetStoreIdResponse( GetStoreIdResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onGetStoreIdResponse( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onCoreSnapshot( CoreSnapshot coreSnapshot )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onCoreSnapshot( requestOutcomeSignal, coreSnapshot );
        }
    }

    Optional<Long> lastResponseTime()
    {
        return Optional.ofNullable( lastResponseTime );
    }

    private void recordLastResponse()
    {
        lastResponseTime = clock.millis();
    }
}
