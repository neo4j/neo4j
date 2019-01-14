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

import java.util.EnumMap;
import java.util.function.Function;

/**
 * Selects an instance given a certain slot and instantiate them lazy.
 * @param <T> type of instance
 */
class LazyInstanceSelector<T> extends InstanceSelector<T>
{
    private final Function<IndexSlot,T> factory;

    /**
     * Empty lazy selector
     *
     * See {@link this#LazyInstanceSelector(EnumMap, Function)}
     */
    LazyInstanceSelector( Function<IndexSlot,T> factory )
    {
        this( new EnumMap<>( IndexSlot.class ), factory );
    }

    /**
     * Lazy selector with initial mapping
     *
     * @param map with initial mapping
     * @param factory {@link Function} for instantiating instances for specific slots.
     */
    LazyInstanceSelector( EnumMap<IndexSlot,T> map, Function<IndexSlot,T> factory )
    {
        super( map );
        this.factory = factory;
    }

    /**
     * Instantiating an instance if it hasn't already been instantiated.
     *
     * See {@link InstanceSelector#select(IndexSlot)}
     */
    @Override
    T select( IndexSlot slot )
    {
        return instances.computeIfAbsent( slot, s ->
        {
            assertOpen();
            return factory.apply( s );
        } );
    }

    /**
     * Returns the instance at the given slot. If the instance at the given {@code slot} hasn't been instantiated yet, {@code null} is returned.
     *
     * @param slot slot to return instance for.
     * @return the instance at the given {@code slot}, or {@code null}.
     */
    T getIfInstantiated( IndexSlot slot )
    {
        return instances.get( slot );
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "This selector has been closed" );
        }
    }
}
