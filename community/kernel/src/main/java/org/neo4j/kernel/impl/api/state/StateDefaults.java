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
package org.neo4j.kernel.impl.api.state;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Iterables;

/**
 * Utility for {@linkplain #get(TxState, Object) retrieving} and
 * {@linkplain #getOrCreate(TxState, Object) initializing} lazy state held in maps in {@link TxState}.
 * <p>
 * {@linkplain #get(TxState, Object) Retrieving} state only guarantees that a readable object is returned, it does not
 * guarantee a writable version. This allows us to return a read-only default value if the state has not been
 * initialized. Only when invoking {@link #getOrCreate(TxState, Object)} do we need to return a writable version, and
 * at this point the state is initialized, if it has not been before, by creating a new instance and putting it in the
 * map.
 * <p>
 * There are two categories of methods in this class, one category concerns the value type, and the other concerns the
 * {@linkplain TxState value holder}. Implementations for methods of these two categories are preferably provided in
 * two stages, as to have each of those participating types contribute their part to the final implementation.
 * See this illustrative example:
 * <code><pre>
 * interface ValueType
 * {
 *     class Mutable extends ValueType {}
 *
 *     // stage one - implement methods concerning the value type
 *     abstract class Defaults extends StateDefaults&lt;String, ValueType, Mutable&gt;
 *     {
 *         private static final ValueType DEFAULT = new ValueType() { ... };
 *         ValueType defaultValue() { return DEFAULT; }
 *         Mutable createValue( String key ) { return new Mutable(); }
 *     }
 * }
 *
 * class ValueHolder
 * {
 *     // stage two - implement methods concerning the reference to the state
 *     private Map&lt;String, ValueType.Mutable&gt; state;
 *     private static final StateDefaults&lt;String, ValueType, ValueType.Mutable&gt; STATE = new ValueType.Defaults()
 *     {
 *         Map&lt;String, ValueType.Mutable&gt; getMap( ValueHolder holder ) { return holder.state; }
 *         void setMap( ValueHolder holder, Map&lt;String, ValueType.Mutable&gt; map ) { holder.state = map; }
 *     };
 * }
 * </pre></code>
 *
 * @param <KEY> The type of the key in the map for the state accessed by an instance of this class
 * @param <RO>  The read-only version of the value type stored in the state
 * @param <RW>  The read/write version of the value type stored in the state
 */
abstract class StateDefaults<KEY, RO, RW extends RO>
{
    final RO get( TxState state, KEY key )
    {
        Map<KEY, RW> map = getMap( state );
        if ( map == null )
        {
            return defaultValue();
        }
        RO value = map.get( key );
        return value == null ? defaultValue() : value;
    }

    final RW getOrCreate( TxState state, KEY key )
    {
        Map<KEY, RW> map = getMap( state );
        if ( map == null )
        {
            setMap( state, map = new HashMap<>() );
        }
        RW value = map.get( key );
        if ( value == null )
        {
            map.put( key, value = createValue( key, state ) );
        }
        return value;
    }

    final Iterable<RO> values( TxState state )
    {
        Map map = getMap( state );
        if ( map == null )
        {
            return Iterables.empty();
        }
        @SuppressWarnings("unchecked")
        Collection<RO> values = map.values();
        return values;
    }

    /** Implemented for the value holder - get the map from the state field. */
    abstract Map<KEY, RW> getMap( TxState state );

    /** Implemented for the value holder - set the map to the state field. */
    abstract void setMap( TxState state, Map<KEY, RW> map );

    /** Implemented for the value type - initializes state by creating a new instance.
     * @param state */
    abstract RW createValue( KEY key, TxState state );

    /** Implemented for the value type - returns a default read-only version of the value type. */
    abstract RO defaultValue();
}
