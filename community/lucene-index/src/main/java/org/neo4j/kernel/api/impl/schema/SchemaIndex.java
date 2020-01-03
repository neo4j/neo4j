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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.schema.verification.UniquenessVerifier;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

/**
 * Partitioned lucene schema index.
 */
public interface SchemaIndex extends DatabaseIndex<IndexReader>
{

    /**
     * Verifies uniqueness of property values present in this index.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyIds the ids of the properties to verify.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(NodePropertyAccessor, int[])
     */
    void verifyUniqueness( NodePropertyAccessor accessor, int[] propertyKeyIds )
            throws IOException, IndexEntryConflictException;

    /**
     * Verifies uniqueness of updated property values.
     *
     * @param accessor the accessor to retrieve actual property values from the store.
     * @param propertyKeyIds the ids of the properties to verify.
     * @param updatedValueTuples the values to check uniqueness for.
     * @throws IndexEntryConflictException if there are duplicates.
     * @throws IOException
     * @see UniquenessVerifier#verify(NodePropertyAccessor, int[], List)
     */
    void verifyUniqueness( NodePropertyAccessor accessor, int[] propertyKeyIds, List<Value[]> updatedValueTuples )
                    throws IOException, IndexEntryConflictException;
}
