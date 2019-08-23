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
package org.neo4j.kernel.impl.locking;

import org.neo4j.hashing.HashFunction;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.values.storable.Value;

public class ResourceIds
{
    // The hash code function we use for index entries and schema names, since Neo4j 4.0.
    private static final HashFunction HASH_40 = HashFunction.incrementalXXH64();
    private static final long HASH_40_INIT = HASH_40.initialise( 0x0123456789abcdefL );

    /**
     * Produces a 64-bit hashcode for locking index entries.
     */
    public static long indexEntryResourceId( long labelId, IndexQuery.ExactPredicate... predicates )
    {
        return indexEntryResourceId_4_x( labelId, predicates );
    }

    /**
     * Produces a 64-bit hashcode for strings that are used as names of schema entities, like indexes and constraints.
     * @param schemaName The name to compute a hash code for.
     * @return The hash code for the given schema name.
     */
    public static long schemaNameResourceId( String schemaName )
    {
        long hash = HASH_40_INIT;
        hash = schemaName.chars().asLongStream().reduce( hash, HASH_40::update );
        return HASH_40.finalise( hash );
    }

    /**
     * This is a stronger, full 64-bit hashing method for schema index entries.
     *
     * @see HashFunction#incrementalXXH64()
     */
    static long indexEntryResourceId_4_x( long labelId, IndexQuery.ExactPredicate[] predicates )
    {
        long hash = HASH_40_INIT;
        hash = HASH_40.update( hash, labelId );

        for ( IndexQuery.ExactPredicate predicate : predicates )
        {
            int propertyKeyId = predicate.propertyKeyId();
            hash = HASH_40.update( hash, propertyKeyId );
            Value value = predicate.value();
            hash = value.updateHash( HASH_40, hash );
        }

        return HASH_40.finalise( hash );
    }
}
