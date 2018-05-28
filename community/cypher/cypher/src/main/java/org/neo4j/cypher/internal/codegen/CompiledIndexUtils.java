/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

import static org.neo4j.cypher.internal.codegen.CompiledConversionUtils.makeValueNeoSafe;
import static org.neo4j.kernel.api.schema.IndexQuery.exact;

/**
 * Utility for dealing with indexes from compiled code
 */
public final class CompiledIndexUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledIndexUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Performs an index seek.
     *
     * @param readOperations The ReadOperation instance to use for seeking
     * @param descriptor The descriptor of the index
     * @param propertyId The property to seek for
     * @param value The value to seek for
     * @return An iterator containing data found in index.
     */
    public static PrimitiveLongIterator indexSeek( ReadOperations readOperations, IndexDescriptor descriptor,
            int propertyId, Object value )
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        if ( value == null )
        {
            return PrimitiveLongCollections.emptyIterator();
        }
        else
        {
            return readOperations.indexQuery( descriptor, exact( propertyId, makeValueNeoSafe( value ) ) );
        }
    }
}
