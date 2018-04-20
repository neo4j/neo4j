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
package org.neo4j.kernel.impl.index.schema.fusion;

/**
 * Selects an instance given a certain slot.
 * @param <T> type of instance
 */
class Selector<T>
{
    private final T[] instances;
    private final ThrowingIntFunction<T,? extends Exception> factory;

    /**
     * @param instances fully instantiated instances so that no factory is needed.
     */
    Selector( T[] instances )
    {
        this( instances, slot ->
        {
            throw new IllegalStateException( "No instantiation expected" );
        } );
    }

    /**
     * @param instances uninstantiated instances, instantiated lazily by the {@code factory}.
     * @param factory {@link ThrowingIntFunction} for instantiating instances for specific slots.
     */
    Selector( T[] instances, ThrowingIntFunction<T,? extends Exception> factory )
    {
        this.instances = instances;
        this.factory = factory;
    }

    T select( int slot )
    {
        if ( instances[slot] == null )
        {
            try
            {
                instances[slot] = factory.apply( slot );
            }
            catch ( Exception e )
            {
                // TODO figure out how to handle this
                throw new RuntimeException( e );
            }
        }
        return instances[slot];
    }

    T getIfInstantiated( int slot )
    {
        return instances[slot];
    }
}
