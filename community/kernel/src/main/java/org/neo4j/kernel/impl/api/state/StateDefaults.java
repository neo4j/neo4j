/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

abstract class StateDefaults<K, RO, RW extends RO>
{
    final RO get( TxState state, K key )
    {
        Map<K, RW> map = getMap( state );
        if ( map == null )
        {
            return defaultValue();
        }
        RO value = map.get( key );
        return value == null ? defaultValue() : value;
    }

    final RW getOrCreate( TxState state, K key )
    {
        Map<K, RW> map = getMap( state );
        if ( map == null )
        {
            setMap( state, map = new HashMap<>() );
        }
        RW value = map.get( key );
        if ( value == null )
        {
            map.put( key, value = createValue( key ) );
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

    abstract Map<K, RW> getMap( TxState state );

    abstract void setMap( TxState state, Map<K, RW> map );

    abstract RW createValue( K key );

    abstract RO defaultValue();
}
