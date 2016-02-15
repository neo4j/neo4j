/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.kernel.api.AccessMode;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.KernelTransactionTestBase;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.exceptions.KernelException;

import static org.junit.Assert.assertNotNull;

public class KernelTransactionAccessModeTest extends KernelTransactionTestBase
{
    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotAllowWriteAccessInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.READ );

        // Expect
        exception.expect( KernelException.class );

        // When
        tx.acquireStatement().dataWriteOperations();
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.READ );

        // Expect
        exception.expect( KernelException.class );

        // When
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.WRITE );

        // Expect
        exception.expect( KernelException.class );

        // When
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldAllowWritesInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.WRITE );

        // When
        DataWriteOperations writes = tx.acquireStatement().dataWriteOperations();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shouldAllowWritesInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.FULL );

        // When
        DataWriteOperations writes = tx.acquireStatement().dataWriteOperations();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shoulAllowSchemaWriteAccessInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction();
        tx.setMode( AccessMode.FULL );

        // When
        SchemaWriteOperations writes = tx.acquireStatement().schemaWriteOperations();

        // Then
        assertNotNull( writes );
    }
}
