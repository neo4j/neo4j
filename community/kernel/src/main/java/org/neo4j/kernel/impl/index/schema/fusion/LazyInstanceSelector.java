/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.util.function.IntFunction;

/**
 * Selects an instance given a certain slot and instantiate them lazy.
 * @param <T> type of instance
 */
class LazyInstanceSelector<T> extends InstanceSelector<T>
{
    private final IntFunction<T> factory;

    /**
     * @param instances uninstantiated instances, instantiated lazily by the {@code factory}
     * @param factory {@link IntFunction} for instantiating instances for specific slots.
     */
    LazyInstanceSelector( T[] instances, IntFunction<T> factory )
    {
        super( instances );
        this.factory = factory;
    }

    /**
     * Instantiating an instance if it hasn't already been instantiated.
     *
     * See {@link InstanceSelector#select(int)}
     */
    @Override
    T select( int slot )
    {
        if ( instances[slot] == null )
        {
            assertOpen();
            instances[slot] = factory.apply( slot );
        }
        return super.select( slot );
    }

    /**
     * Returns the instance at the given slot. If the instance at the given {@code slot} hasn't been instantiated yet, {@code null} is returned.
     *
     * @param slot slot number to return instance for.
     * @return the instance at the given {@code slot}, or {@code null}.
     */
    T getIfInstantiated( int slot )
    {
        return instances[slot];
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This selector has been closed" );
        }
    }
}
