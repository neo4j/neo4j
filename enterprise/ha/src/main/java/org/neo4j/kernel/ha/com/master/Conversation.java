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


import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.impl.locking.Locks;

/**
 * Abstraction to hold all client related info on master side.
 */
public class Conversation implements AutoCloseable
{
    private Locks.Client locks;
    private volatile boolean active = true;
    // since some client locks use pooling we need to be sure that
    // there is no race between client close and stop
    private ReentrantLock lockClientCleanupLock = new ReentrantLock();

    public Conversation( Locks.Client locks )
    {
        this.locks = locks;
    }

    public Locks.Client getLocks()
    {
        return locks;
    }

    @Override
    public void close()
    {
        lockClientCleanupLock.lock();
        try
        {
            if ( locks != null )
            {
                locks.close();
                locks = null;
                active = false;
            }
        }
        finally
        {
            lockClientCleanupLock.unlock();
        }
    }

    public boolean isActive()
    {
        return active;
    }

    public void stop()
    {
        lockClientCleanupLock.lock();
        try
        {
            if ( locks != null )
            {
                locks.stop();
            }
        }
        finally
        {
            lockClientCleanupLock.unlock();
        }
    }
}
