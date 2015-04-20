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
package org.neo4j.kernel.ha.com;

import org.neo4j.com.RequestContext;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class RequestContextFactory extends LifecycleAdapter
{
    private long epoch;
    private final int serverId;
    private final DependencyResolver resolver;
    private TransactionIdStore txIdStore;

    public static final int VOID_EVENT_IDENTIFIER = -3;
    public static final int DEFAULT_EVENT_IDENTIFIER = -1;

    public RequestContextFactory( int serverId, DependencyResolver resolver )
    {
        this.resolver = resolver;
        this.epoch = -1;
        this.serverId = serverId;
    }

    @Override
    public void start() throws Throwable
    {
        this.txIdStore = resolver.resolveDependency( NeoStoreDataSource.class ).getDependencyResolver().resolveDependency( TransactionIdStore.class );
    }

    @Override
    public void stop() throws Throwable
    {
        this.txIdStore = null;
    }

    public void setEpoch( long epoch )
    {
        this.epoch = epoch;
    }

    public RequestContext newRequestContext( long epoch, int machineId, int eventIdentifier )
    {
        long[] lastTx = txIdStore.getLastCommittedTransaction();
        // TODO beware, there's a race between getting tx id and checksum, and changes to last tx
        // it must be fixed
        return new RequestContext( epoch, machineId, eventIdentifier, lastTx[0], lastTx[1] );
    }

    public RequestContext newRequestContext( int eventIdentifier )
    {
        return newRequestContext( epoch, serverId, eventIdentifier );
    }

    public RequestContext newRequestContext()
    {
        return newRequestContext( DEFAULT_EVENT_IDENTIFIER );
    }
}
