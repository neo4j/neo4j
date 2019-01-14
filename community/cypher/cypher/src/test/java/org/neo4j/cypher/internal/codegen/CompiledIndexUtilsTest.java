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
package org.neo4j.cypher.internal.codegen;

import org.junit.Test;

import org.neo4j.internal.kernel.api.CapableIndexReference;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompiledIndexUtilsTest
{

    @Test
    public void shouldCallIndexSeek() throws KernelException
    {

        // GIVEN
        Read read = mock( Read.class );
        CapableIndexReference index = mock( CapableIndexReference.class );
        when( index.properties() ).thenReturn( new int[]{42} );

        // WHEN
        CompiledIndexUtils.indexSeek( read, mock( CursorFactory.class ), index, "hello" );

        // THEN
        verify( read, times( 1 ) ).nodeIndexSeek( any(), any(), any(), any() );
    }

    @Test
    public void shouldHandleNullInIndexSeek() throws KernelException
    {
        // GIVEN
        Read read = mock( Read.class );
        CapableIndexReference index = mock( CapableIndexReference.class );
        when( index.properties() ).thenReturn( new int[]{42} );

        // WHEN
        NodeValueIndexCursor cursor = CompiledIndexUtils.indexSeek( mock( Read.class ), mock( CursorFactory.class ),
                index, null );

        // THEN
        verify( read, never() ).nodeIndexSeek( any(), any(), any() );
        assertFalse( cursor.next() );
    }
}
