/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock;

import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.com.Response;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaXaDataSourceManager;
import org.neo4j.kernel.ha.InstanceAccessGuard;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.GraphProperties;
import org.neo4j.kernel.impl.core.IndexLock;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockManagerImpl;
import org.neo4j.kernel.impl.transaction.LockNotFoundException;
import org.neo4j.kernel.impl.transaction.RagManager;
import org.neo4j.kernel.impl.transaction.TxHook;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.logging.Logging;

public class SlaveLockManager implements LockManager
{
    private final AbstractTransactionManager txManager;
    private final TxHook txHook;
    private final InstanceAccessGuard switchBlock;
    private final Configuration config;
    private final RequestContextFactory requestContextFactory;
    private final LockManagerImpl local;
    private final Master master;
    private final HaXaDataSourceManager xaDsm;

    public static interface Configuration
    {
        long getStateSwitchTimeout();
    }

    public SlaveLockManager( AbstractTransactionManager txManager, TxHook txHook,
                             InstanceAccessGuard switchBlock, Configuration config,
                             RagManager ragManager, RequestContextFactory requestContextFactory, Master master,
                             HaXaDataSourceManager xaDsm )
    {
        this.txManager = txManager;
        this.txHook = txHook;
        this.switchBlock = switchBlock;
        this.config = config;
        this.requestContextFactory = requestContextFactory;
        this.xaDsm = xaDsm;
        this.local = new LockManagerImpl( ragManager );
        this.master = master;
    }

    @Override
    public long getDetectedDeadlockCount()
    {
        return local.getDetectedDeadlockCount();
    }

    @Override
    public void getReadLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getReadLockOnMaster( resource ) )
        {
            local.getReadLock( resource );
        }
    }

    @Override
    public void getReadLock( Object resource, Transaction tx ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getReadLockOnMaster( resource ) )
        {
            local.getReadLock( resource, tx );
        }
    }

    private boolean getReadLockOnMaster( Object resource )
    {
        Response<LockResult> response = null;
        if ( resource instanceof Node )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireNodeReadLock( requestContextFactory.newRequestContext(), ((Node)resource).getId() );
        }
        else if ( resource instanceof Relationship )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireRelationshipReadLock( requestContextFactory.newRequestContext(), ((Relationship)resource).getId() );
        }
        else if ( resource instanceof GraphProperties )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireGraphReadLock( requestContextFactory.newRequestContext() );
        }
        else if ( resource instanceof IndexLock )
        {
            makeSureTxHasBeenInitialized();
            IndexLock indexLock = (IndexLock) resource;
            response = master.acquireIndexReadLock( requestContextFactory.newRequestContext(), indexLock.getIndex(), indexLock.getKey() );
        }
        else
        {
            return true;
        }
        return receiveLockResponse( response );
    }

    private boolean receiveLockResponse( Response<LockResult> response )
    {
        LockResult result = xaDsm.applyTransactions( response );
        switch ( result.getStatus() )
        {
        case DEAD_LOCKED:
            throw new DeadlockDetectedException( result.getDeadlockMessage() );
        case NOT_LOCKED:
            throw new UnsupportedOperationException();
        case OK_LOCKED:
            break;
        default:
            throw new UnsupportedOperationException( result.toString() );
        }

        return true;
    }

    @Override
    public void getWriteLock( Object resource ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getWriteLockOnMaster( resource ) )
        {
            local.getWriteLock( resource );
        }
    }

    @Override
    public void getWriteLock( Object resource, Transaction tx ) throws DeadlockDetectedException, IllegalResourceException
    {
        if ( getWriteLockOnMaster( resource ) )
        {
            local.getWriteLock( resource, tx );
        }
    }

    private boolean getWriteLockOnMaster( Object resource )
    {
        Response<LockResult> response = null;
        if ( resource instanceof Node )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireNodeWriteLock( requestContextFactory.newRequestContext(), ((Node)resource).getId() );
        }
        else if ( resource instanceof Relationship )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireRelationshipWriteLock( requestContextFactory.newRequestContext(), ((Relationship)resource).getId() );
        }
        else if ( resource instanceof GraphProperties )
        {
            makeSureTxHasBeenInitialized();
            response = master.acquireGraphWriteLock( requestContextFactory.newRequestContext() );
        }
        else if ( resource instanceof IndexLock )
        {
            makeSureTxHasBeenInitialized();
            IndexLock indexLock = (IndexLock) resource;
            response = master.acquireIndexWriteLock( requestContextFactory.newRequestContext(), indexLock.getIndex(), indexLock.getKey() );
        }
        else
        {
            return true;
        }
        
        return receiveLockResponse( response );
    }
    
    @Override
    public void releaseReadLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseReadLock( resource, tx );
    }

    @Override
    public void releaseWriteLock( Object resource, Transaction tx ) throws LockNotFoundException,
            IllegalResourceException
    {
        local.releaseWriteLock( resource, tx );
    }

    @Override
    public void dumpLocksOnResource( Object resource, Logging logging )
    {
        local.dumpLocksOnResource( resource, logging );
    }

    @Override
    public List<LockInfo> getAllLocks()
    {
        return local.getAllLocks();
    }

    @Override
    public List<LockInfo> getAwaitedLocks( long minWaitTime )
    {
        return local.getAwaitedLocks( minWaitTime );
    }

    @Override
    public void dumpRagStack( Logging logging )
    {
        local.dumpRagStack( logging );
    }

    @Override
    public void dumpAllLocks( Logging logging )
    {
        local.dumpAllLocks( logging );
    }

    private void makeSureTxHasBeenInitialized()
    {
        int eventIdentifier = txManager.getEventIdentifier();
        if ( !txManager.getTransactionState().hasLocks() )
        {
            if ( !switchBlock.await( config.getStateSwitchTimeout() ) )
            {
                // TODO Specific exception instead?
                throw new RuntimeException( "Timed out waiting for database to switch state" );
            }

            txHook.initializeTransaction( eventIdentifier );
        }
    }
}
