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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.junit.Assert.assertNotNull;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class KernelTransactionSecurityContextTest extends KernelTransactionTestBase
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotAllowReadsInNoneMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.dataRead();
    }

    @Test
    public void shouldNotAllowTokenReadsInNoneMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.tokenRead();
    }

    @Test
    public void shouldNotAllowWritesInNoneMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.dataWrite();
    }

    @Test
    public void shouldNotAllowSchemaWritesInNoneMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.schemaWrite();
    }

    @Test
    public void shouldAllowReadsInReadMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    public void shouldNotAllowWriteAccessInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.dataWrite();
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.schemaWrite();
    }

    @Test
    public void shouldNotAllowReadAccessInWriteOnlyMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.dataRead();
    }

    @Test
    public void shouldNotAllowTokenReadAccessInWriteOnlyMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.tokenRead();
    }

    @Test
    public void shouldAllowWriteAccessInWriteOnlyMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInWriteOnlyMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.schemaWrite();
    }

    @Test
    public void shouldAllowReadsInWriteMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    public void shouldAllowWritesInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.schemaWrite();
    }

    @Test
    public void shouldAllowReadsInFullMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    public void shouldAllowWritesInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shouldAllowSchemaWriteAccessInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        SchemaWrite writes = tx.schemaWrite();

        // Then
        assertNotNull( writes );
    }
}
