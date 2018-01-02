/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking.community;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class CommunityLockManger extends LifecycleAdapter implements Locks
{
    private final LockManagerImpl manager = new LockManagerImpl( new RagManager() );
    private volatile boolean closed;

    @Override
    public Client newClient()
    {
        // We check this volatile closed flag here, which may seem like a contention overhead, but as the time
        // of writing we apply pooling of transactions and in extension pooling of lock clients,
        // so this method is called very rarely.
        if ( closed )
        {
            throw new IllegalStateException( this + " already closed" );
        }
        return new CommunityLockClient( manager );
    }

    @Override
    public void accept( final Visitor visitor )
    {
        manager.accept( new org.neo4j.helpers.collection.Visitor<RWLock, RuntimeException>()
        {
            @Override
            public boolean visit( RWLock element ) throws RuntimeException
            {
                Object resource = element.resource();
                if(resource instanceof LockResource)
                {
                    LockResource lockResource = (LockResource)resource;
                    visitor.visit( lockResource.type(), lockResource.resourceId(),
                            element.describe(), element.maxWaitTime(), System.identityHashCode( lockResource ) );
                }
                return false;
            }
        } );
    }

    @Override
    public void shutdown() throws Throwable
    {
        closed = true;
    }
}
