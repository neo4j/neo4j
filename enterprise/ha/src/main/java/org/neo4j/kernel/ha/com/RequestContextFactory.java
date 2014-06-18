/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.com;

import org.neo4j.com.RequestContext;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionMetadataCache;
import org.neo4j.kernel.impl.transaction.xaframework.LogicalTransactionStore;

public class RequestContextFactory
{
    private long epoch;
    private final int serverId;
    private final DependencyResolver resolver;
    private final LogicalTransactionStore txStore;
    private final TransactionIdStore txIdStore;

    public RequestContextFactory( int serverId, DependencyResolver resolver, LogicalTransactionStore txStore,
                                  TransactionIdStore txIdStore)
    {
        this.resolver = resolver;
        this.epoch = -1;
        this.serverId = serverId;
        this.txStore = txStore;
        this.txIdStore = txIdStore;
    }

    public void setEpoch( long epoch )
    {
        this.epoch = epoch;
    }

    public RequestContext newRequestContext( long sessionId, int machineId, int eventIdentifier )
    {
        long latestTxId = txIdStore.getLastCommittingTransactionId();
        TransactionMetadataCache.TransactionMetadata txMetadata = txStore.getMetadataFor( latestTxId );
        return new RequestContext(
                sessionId, machineId, eventIdentifier, latestTxId, txMetadata.getMasterId(), txMetadata.getAuthorId() );
    }

    public RequestContext newRequestContext( int eventIdentifier )
    {
        return newRequestContext( epoch, serverId, eventIdentifier );
    }

    public RequestContext newRequestContext()
    {
        return newRequestContext( -1 );
    }
}
