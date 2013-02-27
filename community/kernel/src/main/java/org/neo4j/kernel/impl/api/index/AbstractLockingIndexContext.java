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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.kernel.api.IndexState;

public abstract class AbstractLockingIndexContext extends AbstractDelegatingIndexContext
{
    private final ReadWriteLock lock;

    public AbstractLockingIndexContext()
    {
        this.lock = new ReentrantReadWriteLock( );
    }

    protected ReadWriteLock getLock()
    {
        return lock;
    }

    public void create()
   {
       lock.readLock().lock();
       try {
           super.create();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void update( Iterable<NodePropertyUpdate> updates )
   {
       lock.readLock().lock();
       try {
           super.update( updates );
       }
       finally {
           lock.readLock().unlock();
       }
   }

   public void drop()
   {
       lock.readLock().lock();
       try {
           super.drop();
       }
       finally {
           lock.readLock().unlock();
       }
   }

   @Override
   public IndexState getState()
   {
       lock.readLock().lock();
       try {
           return super.getState();
       }
       finally {
           lock.readLock().unlock();
       }
   }
}
