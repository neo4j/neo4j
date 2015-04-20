/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public class SoftValue<K, V> extends SoftReference<V> implements ReferenceWithKey<K, V>
{
    public static Factory SOFT_VALUE_FACTORY = new Factory()
    {
        @Override
        public <FK, FV> SoftValue<FK, FV> newReference( FK key, FV value, ReferenceQueue<? super FV> queue )
        {
            return new SoftValue<>( key, value, queue );
        }
    };

    public final K key;

    public SoftValue( K key, V value, ReferenceQueue<? super V> queue )
    {
        super( value, queue );
        this.key = key;
    }

    @Override
    public K key()
    {
        return key;
    }
}
