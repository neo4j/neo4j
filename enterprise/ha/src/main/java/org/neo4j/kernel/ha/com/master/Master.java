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
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionRepresentation;

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
     * This is a misleading method name. This is the only mechanism available for committing ad-hoc transactions
     * remotely on a master database. Calling this method will validate, persist to log and apply changes to stores on
     * the master.
     *
     * TODO: Change the name of this method
     */
    Response<Long> commitSingleResourceTransaction( RequestContext context, TransactionRepresentation channel ) throws IOException, TransactionFailureException;

    /**
     * This is a misleading method name, it has nothing to do with transactions.
     * Calling this method will create a lock client on the master that can be used on behalf of the callee to grab
     * cluster-global locks.
     *
     * TODO: Change the name of this
     */
    Response<Void> initializeTx( RequestContext context );

    /**
     * This is a misleading method name it has nothing to do with transactions.
     * Calling this will release all locks held on the master on the behalf of the
     * specified context. The "success" parameter is ignored.
     *
     * TODO: Change the name of this
     */
    Response<Void> finishTransaction( RequestContext context, boolean success );

    /**
     * Gets the master id for a given txId, also a checksum for that tx.
     *
     * @param txId      the transaction id to get the data for.
     * @param myStoreId clients store id.
     * @return the master id for a given txId, also a checksum for that tx.
     */
    Response<HandshakeResult> handshake( long txId, StoreId myStoreId );
    Response<Void> pushTransaction( RequestContext context, long tx );

    Response<Void> pullUpdates( RequestContext context );

    Response<Void> copyStore( RequestContext context, StoreWriter writer );

    Response<Void> copyTransactions( RequestContext context, String dsName,
                                     long startTxId, long endTxId );

    Response<LockResult> acquireExclusiveLock( RequestContext context, Locks.ResourceType type, long... resourceIds );
    Response<LockResult> acquireSharedLock( RequestContext context, Locks.ResourceType type, long... resourceIds );
}
