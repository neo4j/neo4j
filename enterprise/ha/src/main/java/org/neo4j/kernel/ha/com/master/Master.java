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
package org.neo4j.kernel.ha.com.master;

import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * Represents the master-side of the HA communication between master and slave.
 * A master will receive calls to these methods from slaves when they do stuff.
 */
public interface Master
{
    Response<IdAllocation> allocateIds( RequestContext context, IdType idType );

    Response<Integer> createRelationshipType( RequestContext context, String name );

    Response<Integer> createPropertyKey( RequestContext context, String name );

    Response<Integer> createLabel( RequestContext context, String name );

    /**
     * Calling this method will validate, persist to log and apply changes to stores on
     * the master.
     */
    Response<Long> commit( RequestContext context, TransactionRepresentation channel ) throws IOException, TransactionFailureException;

    /**
     * Calling this method will create a new session with the cluster lock manager and associate that
     * session with the provided {@link RequestContext}.
     */
    Response<Void> newLockSession( RequestContext context ) throws TransactionFailureException;

    /**
     * Calling this will end the current lock session (identified by the {@link RequestContext}),
     * releasing all cluster-global locks held.
     */
    Response<Void> endLockSession( RequestContext context, boolean success );

    /**
     * Gets the master id for a given txId, also a checksum for that tx.
     *
     * @param txId      the transaction id to get the data for.
     * @param myStoreId clients store id.
     * @return the master id for a given txId, also a checksum for that tx.
     */
    Response<HandshakeResult> handshake( long txId, StoreId myStoreId );

    Response<Void> pullUpdates( RequestContext context );

    Response<Void> copyStore( RequestContext context, StoreWriter writer );

    Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType type, long... resourceIds );

    Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType type, long... resourceIds );


}
