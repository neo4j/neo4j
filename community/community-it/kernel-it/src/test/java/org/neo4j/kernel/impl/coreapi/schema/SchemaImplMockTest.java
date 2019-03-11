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
package org.neo4j.kernel.impl.coreapi.schema;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Exceptions;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SchemaImplMockTest
{
    private static final Exception cause = new Exception( "Kilroy made it" );

    @Test
    void includeCauseOfFailure() throws IndexNotFoundKernelException
    {
        // given
        IndexDefinitionImpl indexDefinition = mockIndexDefinition();
        when( indexDefinition.toString() ).thenReturn( "IndexDefinition( of-some-sort )" );
        KernelTransaction kernelTransaction = mockKernelTransaction();
        SchemaImpl schema = new SchemaImpl( () -> kernelTransaction );

        // when
        IllegalStateException e = assertThrows( IllegalStateException.class, () -> schema.awaitIndexOnline( indexDefinition, 1, TimeUnit.MINUTES ) );

        // then
        assertThat( e.getMessage(), Matchers.containsString( indexDefinition.toString() ) );
        assertThat( e.getMessage(), Matchers.containsString( Exceptions.stringify( cause ) ) );
    }

    private static IndexDefinitionImpl mockIndexDefinition()
    {
        IndexDefinitionImpl indexDefinition = mock( IndexDefinitionImpl.class );
        when( indexDefinition.getIndexReference() ).thenReturn( IndexReference.NO_INDEX );
        return indexDefinition;
    }

    private static KernelTransaction mockKernelTransaction() throws IndexNotFoundKernelException
    {
        SchemaRead schemaRead = mock( SchemaRead.class );
        when( schemaRead.indexGetState( any( IndexReference.class ) ) ).thenReturn( InternalIndexState.FAILED );
        when( schemaRead.indexGetFailure( any( IndexReference.class ) ) ).thenReturn( Exceptions.stringify( cause ) );

        KernelTransaction kt = mock( KernelTransaction.class );
        when( kt.tokenRead() ).thenReturn( mock( TokenRead.class ) );
        when( kt.schemaRead() ).thenReturn( schemaRead );
        when( kt.isTerminated() ).thenReturn( false );
        when( kt.acquireStatement() ).thenReturn( mock( Statement.class ) );
        return kt;
    }
}
