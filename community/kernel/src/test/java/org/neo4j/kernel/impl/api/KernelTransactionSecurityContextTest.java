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

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

class KernelTransactionSecurityContextTest extends KernelTransactionTestBase
{
    @Test
    void shouldAllowReadsInAccessMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.access() );

        // This is allowed, but will see an empty graph
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    void shouldAllowTokenReadsInAccessMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.access() );

        // When
        TokenRead tokenRead = tx.tokenRead();

        // Then
        assertNotNull( tokenRead );
    }

    @Test
    void shouldNotAllowWritesInAccessMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.access() );

        assertThrows( AuthorizationViolationException.class, tx::dataWrite );
    }

    @Test
    void shouldNotAllowSchemaWritesInAccessMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.access() );

        assertThrows( AuthorizationViolationException.class, tx::schemaWrite );
    }

    @Test
    void shouldAllowReadsInReadMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    void shouldNotAllowWriteAccessInReadMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        assertThrows( AuthorizationViolationException.class, tx::dataWrite );
    }

    @Test
    void shouldNotAllowSchemaWriteAccessInReadMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        assertThrows( AuthorizationViolationException.class, tx::schemaWrite );
    }

    @Test
    void shouldAllowReadAccessInWriteOnlyMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // When
        Read reads = tx.dataRead();

        // Then
        // This is allowed, but will see an empty graph
        assertNotNull( reads );
    }

    @Test
    void shouldAllowTokenReadAccessInWriteOnlyMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // When
        TokenRead tokenRead = tx.tokenRead();

        // Then
        assertNotNull( tokenRead );
    }

    @Test
    void shouldAllowWriteAccessInWriteOnlyMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    void shouldNotAllowSchemaWriteAccessInWriteOnlyMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        assertThrows( AuthorizationViolationException.class, tx::schemaWrite );
    }

    @Test
    void shouldAllowReadsInWriteMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    void shouldAllowWritesInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    void shouldNotAllowSchemaWriteAccessInWriteMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        assertThrows( AuthorizationViolationException.class, tx::schemaWrite );
    }

    @Test
    void shouldAllowReadsInFullMode()
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        Read reads = tx.dataRead();

        // Then
        assertNotNull( reads );
    }

    @Test
    void shouldAllowWritesInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        Write writes = tx.dataWrite();

        // Then
        assertNotNull( writes );
    }

    @Test
    void shouldAllowSchemaWriteAccessInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        SchemaWrite writes = tx.schemaWrite();

        // Then
        assertNotNull( writes );
    }
}
