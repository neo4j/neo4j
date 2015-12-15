/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.catchup.tx.edge;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.coreedge.catchup.storecopy.edge.CoreClient;
import org.neo4j.coreedge.catchup.storecopy.edge.StoreCopyFailedException;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;

public class TxPullClient
{
    private final CoreClient coreClient;

    public TxPullClient( CoreClient coreClient )
    {
        this.coreClient = coreClient;
    }

    public long pullTransactions( AdvertisedSocketAddress from, long startTxId, TxPullResponseListener txPullResponseListener )
            throws StoreCopyFailedException
    {
        coreClient.addTxPullResponseListener( txPullResponseListener );

        CompletableFuture<Long> txId = new CompletableFuture<>();

        TxStreamCompleteListener streamCompleteListener = txId::complete;
        coreClient.addTxStreamCompleteListener( streamCompleteListener );

        try
        {
            coreClient.pollForTransactions( from, startTxId );
            return txId.get();
        }
        catch ( InterruptedException | ExecutionException e )
        {
            throw new StoreCopyFailedException( e );
        }
        finally
        {
            coreClient.removeTxPullResponseListener( txPullResponseListener );
            coreClient.removeTxStreamCompleteListener( streamCompleteListener );
        }
    }
}
