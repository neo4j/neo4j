/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.junit.Assert.assertNotNull;
import static org.neo4j.kernel.api.security.SecurityContext.AUTH_DISABLED;

public class KernelTransactionSecurityContextTest extends KernelTransactionTestBase
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldNotAllowReadsInNoneMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.acquireStatement().readOperations();
    }

    @Test
    public void shouldNotAllowWritesInNoneMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.acquireStatement().dataWriteOperations();
    }

    @Test
    public void shouldNotAllowSchemaWritesInNoneMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.none() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldAllowReadsInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // When
        ReadOperations reads = tx.acquireStatement().readOperations();

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
        tx.acquireStatement().dataWriteOperations();
    }

    @Test
    public void shouldNotAllowSchemaWriteAccessInReadMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.read() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldNotAllowReadAccessInWriteOnlyMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // Expect
        exception.expect( AuthorizationViolationException.class );

        // When
        tx.acquireStatement().readOperations();
    }

    @Test
    public void shouldAllowWriteAccessInWriteOnlyMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.writeOnly() );

        // When
        DataWriteOperations writes = tx.acquireStatement().dataWriteOperations();

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
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldAllowReadsInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        ReadOperations reads = tx.acquireStatement().readOperations();

        // Then
        assertNotNull( reads );
    }

    @Test
    public void shouldAllowWritesInWriteMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AnonymousContext.write() );

        // When
        DataWriteOperations writes = tx.acquireStatement().dataWriteOperations();

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
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldAllowReadsInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        ReadOperations reads = tx.acquireStatement().readOperations();

        // Then
        assertNotNull( reads );
    }

    @Test
    public void shouldAllowWritesInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        DataWriteOperations writes = tx.acquireStatement().dataWriteOperations();

        // Then
        assertNotNull( writes );
    }

    @Test
    public void shouldAllowSchemaWriteAccessInFullMode() throws Throwable
    {
        // Given
        KernelTransactionImplementation tx = newTransaction( AUTH_DISABLED );

        // When
        SchemaWriteOperations writes = tx.acquireStatement().schemaWriteOperations();

        // Then
        assertNotNull( writes );
    }
}
