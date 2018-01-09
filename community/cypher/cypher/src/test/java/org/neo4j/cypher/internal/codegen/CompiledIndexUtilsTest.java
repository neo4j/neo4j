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
package org.neo4j.cypher.internal.codegen;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.api.index.IndexQueryHelper.exact;

public class CompiledIndexUtilsTest
{
    @Test
    public void shouldCallIndexSeek()
            throws EntityNotFoundException, IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        // GIVEN
        ReadOperations read = mock( ReadOperations.class );

        // WHEN
        IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 12, 42 );
        CompiledIndexUtils.indexSeek( read, descriptor, 42, "hello" );

        // THEN
        verify( read, times( 1 ) ).indexQuery( descriptor, exact( 42, "hello" ) );
    }

    @Test
    public void shouldHandleNullInIndexSeek()
            throws EntityNotFoundException, IndexNotApplicableKernelException, IndexNotFoundKernelException
    {
        // GIVEN
        ReadOperations read = mock( ReadOperations.class );

        // WHEN
        IndexDescriptor descriptor = IndexDescriptorFactory.forLabel( 12, 42 );
        PrimitiveLongIterator iterator =
                CompiledIndexUtils.indexSeek( read, descriptor, 42, null );

        // THEN
        verify( read, never() ).indexQuery( any( ), any(  ) );
        assertFalse( iterator.hasNext() );
    }
}
