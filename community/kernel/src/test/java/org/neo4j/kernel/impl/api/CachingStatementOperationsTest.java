/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.kernel.api.KernelStatement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.api.PrimitiveIntIteratorForArray.primitiveIntIteratorToIntArray;
import static org.neo4j.kernel.impl.api.StatementOperationsTestHelper.mockedState;

public class CachingStatementOperationsTest
{
    @Test
    public void shouldGetCachedLabelsIfCached() throws EntityNotFoundException
    {
        // GIVEN
        long nodeId = 3;
        int[] labels = new int[] {1, 2, 3};
        PersistenceCache cache = mock( PersistenceCache.class );
        when( cache.nodeGetLabels( any( KernelStatement.class ), eq( nodeId ), any( CacheLoader.class ) ) ).thenReturn( labels );
        EntityReadOperations entityReadOperations = mock( EntityReadOperations.class );
        SchemaReadOperations schemaReadOperations = mock( SchemaReadOperations.class );
        CachingStatementOperations context = new CachingStatementOperations(
                entityReadOperations, schemaReadOperations, cache, null );

        // WHEN
        PrimitiveIntIterator receivedLabels = context.nodeGetLabels( mockedState(), nodeId );

        // THEN
        assertArrayEquals( labels, primitiveIntIteratorToIntArray( receivedLabels ) );
    }
}
