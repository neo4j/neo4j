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
package org.neo4j.kernel.api.impl.schema.verification;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.values.storable.Value;

/**
 * A component that verifies uniqueness of values in a lucene index.
 * During uniqueness constraint creation we ensure that already existing data is unique using
 * {@link #verify(PropertyAccessor, int[])}.
 * Since updates can be applied while index is being populated we need to verify them as well.
 * Verification does not handle that automatically. They need to be collected in some way and then checked by
 * {@link #verify(PropertyAccessor, int[], List)}.
 */
public interface UniquenessVerifier extends Closeable
{
    /**
     * Verifies uniqueness of existing data.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propKeyIds the ids of the properties to verify.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException when Lucene throws {@link IOException}.
     */
    void verify( PropertyAccessor accessor, int[] propKeyIds ) throws IndexEntryConflictException, IOException;

    /**
     * Verifies uniqueness of given values and existing data.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propKeyIds the ids of the properties to verify.
     * @param updatedValueTuples the values to check uniqueness for.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException when Lucene throws {@link IOException}.
     */
    void verify( PropertyAccessor accessor, int[] propKeyIds, List<Value[]> updatedValueTuples )
            throws IndexEntryConflictException, IOException;
}
