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
package org.neo4j.kernel.ha.com.master;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.lock.ResourceType;

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
    Response<Long> commit( RequestContext context, TransactionRepresentation channel ) throws TransactionFailureException;

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

    Response<LockResult> acquireExclusiveLock( RequestContext context, ResourceType type, long... resourceIds );

    Response<LockResult> acquireSharedLock( RequestContext context, ResourceType type, long... resourceIds );

}
