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
package org.neo4j.io.pagecache.buffer;

import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

/**
 * Intermediate temporary buffer that is used during page file flushing.
 *
 * @see MuninnPageCache
 */
public interface NativeIOBuffer extends AutoCloseable
{
    /**
     * @return true if usage of native io buffer is enabled
     */
    boolean isEnabled();

    /**
     * @return when buffer is full and there is no more capacity to hold more data. Disabled buffer does not have any capacity.
     * @param used number of bytes already used
     * @param requestSize size of memory request to expect
     */
    boolean hasMoreCapacity( int used, int requestSize );

    /**
     * Access to this method should always be guarded by enable check. In case of buffer is disabled any number can be returned.
     * @return underlying buffer memory address
     */
    long getAddress();

    @Override
    void close();
}
