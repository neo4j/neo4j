/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.core.TransactionState;

public class BeansAPITransaction implements Transaction
{
    private final TransactionContext txCtx;
    private final TransactionState state;
    private final ThreadToStatementContextBridge bridge;

    public BeansAPITransaction( TransactionContext txCtx, TransactionState state, ThreadToStatementContextBridge bridge )
    {
        this.txCtx = txCtx;
        this.state = state;
        this.bridge = bridge;
    }

    @Override
    public void failure()
    {
        txCtx.failure();
    }

    @Override
    public void success()
    {
        txCtx.success();
    }

    @Override
    public void finish()
    {
        txCtx.finish();
        bridge.clearThisThread();
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return state.acquireWriteLock( entity );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return state.acquireReadLock( entity );
    }
}
