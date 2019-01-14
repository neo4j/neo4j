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
package org.neo4j.internal.kernel.api.helpers;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeoutException;

import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexesTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    private final Register.DoubleLongRegister register = Registers.newDoubleLongRegister();

    @Test
    public void shouldNotTimeOutIfNoIndexes() throws Throwable
    {
        // Given
        final SchemaRead schemaRead = schemaWithIndexes();

        // Then no exception
        Indexes.awaitResampling( schemaRead, 0 );
    }

    @Test
    public void shouldNotTimeOutIfNoUpdates() throws Throwable
    {
        // Given
        IndexReference index =  mock( IndexReference.class );
        final SchemaRead schemaRead = schemaWithIndexes( index );
        setUpdates( schemaRead, 0 );

        // Then no exception
        Indexes.awaitResampling( schemaRead, 0 );
    }
    @Test
    public void shouldAwaitIndexResampling() throws Throwable
    {
        // Given
        IndexReference index =  mock( IndexReference.class );
        final SchemaRead schemaRead = schemaWithIndexes( index );
        setUpdates( schemaRead, 1, 2, 3, 0 );

        // Then no exception
        Indexes.awaitResampling( schemaRead, 60 );
    }

    @Test
    public void shouldAwaitIndexResamplingForHeavyLoad() throws Throwable
    {
        // Given
        IndexReference index =  mock( IndexReference.class );
        final SchemaRead schemaRead = schemaWithIndexes( index );
        setUpdates( schemaRead, 1, 2, 3, 2 );  // <- updates went down but didn't reach the first seen value

        // Then no exception
        Indexes.awaitResampling( schemaRead, 60 );
    }

    @Test
    public void shouldTimeout() throws Throwable
    {
        // Given
        IndexReference index =  mock( IndexReference.class );
        final SchemaRead schemaRead = schemaWithIndexes( index );
        setUpdates( schemaRead, 1, 1, 1 );

        // Then
        exception.expect( TimeoutException.class );
        Indexes.awaitResampling( schemaRead, 1 );
    }

    private SchemaRead schemaWithIndexes( IndexReference... indexes )
    {
        final SchemaRead schemaRead = mock( SchemaRead.class );
        when( schemaRead.indexesGetAll() ).thenReturn( Iterators.iterator( indexes ) );
        return schemaRead;
    }

    private void setUpdates( SchemaRead schemaRead, int... updates ) throws IndexNotFoundKernelException
    {
        when( schemaRead.indexUpdatesAndSize( any( IndexReference.class ), any( Register.DoubleLongRegister.class ) ) ).thenAnswer(
                new Answer<Register.DoubleLongRegister>()
                {
                    private int i;

                    @Override
                    public Register.DoubleLongRegister answer( InvocationOnMock invocationOnMock ) throws Throwable
                    {
                        Register.DoubleLongRegister r = invocationOnMock.getArgument( 1 );
                        r.write( updates[i], 0 );
                        i = (i + 1) % updates.length;
                        return r;
                    }
                } );
    }
}
