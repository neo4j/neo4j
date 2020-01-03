/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.store;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

class InaccessibleLock implements Lock
{
    private final String message;

    InaccessibleLock( String message )
    {
        this.message = message;
    }

    @Override
    public void lock()
    {
        throw new IllegalStateException( message );
    }

    @Override
    public void lockInterruptibly()
    {
        throw new IllegalStateException( message );
    }

    @Override
    public boolean tryLock()
    {
        return false;
    }

    @Override
    public boolean tryLock( long time, TimeUnit unit )
    {
        return false;
    }

    @Override
    public void unlock()
    {
    }

    @Override
    public Condition newCondition()
    {
        throw new UnsupportedOperationException( message );
    }
}
