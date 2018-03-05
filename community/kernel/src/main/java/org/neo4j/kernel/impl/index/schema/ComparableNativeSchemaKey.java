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
package org.neo4j.kernel.impl.index.schema;

import java.util.Comparator;

/**
 * Adds comparability to NativeSchemaKey.
 *
 * @param <SELF> the type of the concrete implementing subclass
 */
abstract class ComparableNativeSchemaKey<SELF extends ComparableNativeSchemaKey> extends NativeSchemaKey
{

    /**
     * Compares the value of this key to that of another key.
     * This method is expected to be called in scenarios where inconsistent reads may happen (and later retried).
     *
     * @param other the {@link LocalTimeSchemaKey} to compare to.
     * @return comparison against the {@code other} {@link LocalTimeSchemaKey}.
     */
    abstract int compareValueTo( SELF other );

    static <T extends ComparableNativeSchemaKey<T>> Comparator<T> UNIQUE()
    {
        return ( o1, o2 ) -> {
            int comparison = o1.compareValueTo( o2 );
            if ( comparison == 0 )
            {
                // This is a special case where we need also compare entityId to support inclusive/exclusive
                if ( o1.getCompareId() || o2.getCompareId() )
                {
                    return Long.compare( o1.getEntityId(), o2.getEntityId() );
                }
            }
            return comparison;
        };
    }

    static <T extends ComparableNativeSchemaKey<T>> Comparator<T> NON_UNIQUE()
    {
        return ( o1, o2 ) -> {
            int comparison = o1.compareValueTo( o2 );
            return comparison != 0 ? comparison : Long.compare( o1.getEntityId(), o2.getEntityId() );
        };
    }
}
