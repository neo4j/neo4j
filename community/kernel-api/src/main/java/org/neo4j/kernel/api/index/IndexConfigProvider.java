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
package org.neo4j.kernel.api.index;

import java.util.Collections;
import java.util.Map;

import org.neo4j.values.storable.Value;

/**
 * Inherited by {@link IndexAccessor} and {@link IndexPopulator}.
 */
public interface IndexConfigProvider
{
    /**
     * Get index configurations used by this index at runtime.
     *
     * @return {@link Map} describing index configurations for this index.
     */
    default Map<String,Value> indexConfig()
    {
        return Collections.emptyMap();
    }

    /**
     * Add all entries from source to target and make sure
     * @param target {@link Map} to which entries are added.
     * @param source {@link Map} from which entries are taken, will not be modified.
     */
    static void putAllNoOverwrite( Map<String,Value> target, Map<String,Value> source )
    {
        for ( Map.Entry<String,Value> partEntry : source.entrySet() )
        {
            String key = partEntry.getKey();
            Value value = partEntry.getValue();
            if ( target.containsKey( key ) )
            {
                throw new IllegalStateException( String.format( "Adding config would overwrite existing value: key=%s, newValue=%s, oldValue=%s",
                        key, value, target.get( key ) ) );
            }
            target.put( key, value );
        }
    }
}
