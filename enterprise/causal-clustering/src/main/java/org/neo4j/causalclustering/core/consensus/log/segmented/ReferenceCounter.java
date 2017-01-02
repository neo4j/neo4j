/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Keeps a reference count using CAS.
 */
class ReferenceCounter
{
    private static final int DISPOSED_VALUE = -1;

    private AtomicInteger count = new AtomicInteger();

    boolean increase()
    {
        while ( true )
        {
            int pre = count.get();
            if ( pre == DISPOSED_VALUE )
            {
                return false;
            }
            else if ( count.compareAndSet( pre, pre + 1 ) )
            {
                return true;
            }
        }
    }

    void decrease()
    {
        while ( true )
        {
            int pre = count.get();
            if ( pre <= 0 )
            {
                throw new IllegalStateException( "Illegal count: " + pre );
            }
            else if ( count.compareAndSet( pre, pre - 1 ) )
            {
                return;
            }
        }
    }

    /**
     * Idempotently try to dispose this reference counter.
     *
     * @return True if the reference counter was or is now disposed.
     */
    boolean tryDispose()
    {
        return count.get() == DISPOSED_VALUE || count.compareAndSet( 0, DISPOSED_VALUE );
    }

    public int get()
    {
        return count.get();
    }
}
