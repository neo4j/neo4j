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
package org.neo4j.memory;

public class DelegatingMemoryPool implements MemoryPool
{
    private final MemoryPool delegate;

    DelegatingMemoryPool( MemoryPool delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void reserveHeap( long bytes )
    {
        delegate.reserveHeap( bytes );
    }

    @Override
    public void reserveNative( long bytes )
    {
        delegate.reserveNative( bytes );
    }

    @Override
    public void releaseHeap( long bytes )
    {
        delegate.releaseHeap( bytes );
    }

    @Override
    public void releaseNative( long bytes )
    {
        delegate.releaseNative( bytes );
    }

    @Override
    public long totalSize()
    {
        return delegate.totalSize();
    }

    @Override
    public long usedHeap()
    {
        return delegate.usedHeap();
    }

    @Override
    public long usedNative()
    {
        return delegate.usedNative();
    }

    @Override
    public long totalUsed()
    {
        return delegate.totalUsed();
    }

    @Override
    public long free()
    {
        return delegate.free();
    }

    @Override
    public void setSize( long size )
    {
        delegate.setSize( size );
    }
}
