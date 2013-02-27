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

public class FlippableIndexContext extends AbstractLockingIndexContext
{
    private static final Runnable NO_OP = new Runnable()
    {
        @Override
        public void run()
        {
        }
    };

    private IndexContext flipTarget = null;
    private IndexContext delegate;

    public FlippableIndexContext()
    {
        this( null );
    }

    public FlippableIndexContext( IndexContext originalDelegate )
    {
        this.delegate = originalDelegate;
    }

    public IndexContext getDelegate()
    {
        getLock().readLock().lock();
        try {
            return delegate;
        }
        finally {
             getLock().readLock().unlock();
        }
    }


    public void setFlipTarget( IndexContext flipTarget )
    {
        getLock().writeLock().lock();
        try {
            this.flipTarget = flipTarget;
        }
        finally {
            getLock().writeLock().unlock();
        }
    }

    public void flip()
    {
        flip( NO_OP );
    }

    public void flip( Runnable actionDuringFlip )
    {
        getLock().writeLock().lock();
        try {
            actionDuringFlip.run();
            this.delegate = flipTarget;
        }
        finally {
            getLock().writeLock().unlock();
        }
    }
}
