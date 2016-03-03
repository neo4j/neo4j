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
package org.neo4j.kernel.api;

import org.junit.Test;

import org.neo4j.kernel.api.security.AccessMode;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import static org.neo4j.kernel.api.KernelTransactionFactory.kernelTransaction;

public class TransactionStatementSharingTest
{
    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.READ );
        ReadOperations stmt1 = tx.acquireStatement().readOperations();

        // when
        ReadOperations stmt2 = tx.acquireStatement().readOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.WRITE );
        ReadOperations stmt1 = tx.acquireStatement().readOperations();

        // when
        DataWriteOperations stmt2 = tx.acquireStatement().dataWriteOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentReadStatementAndSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.FULL );
        ReadOperations stmt1 = tx.acquireStatement().readOperations();

        // when
        SchemaWriteOperations stmt2 = tx.acquireStatement().schemaWriteOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentDataStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.WRITE );
        DataWriteOperations stmt1 = tx.acquireStatement().dataWriteOperations();

        // when
        ReadOperations stmt2 = tx.acquireStatement().readOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentDataStatementAndDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.WRITE );
        DataWriteOperations stmt1 = tx.acquireStatement().dataWriteOperations();

        // when
        DataWriteOperations stmt2 = tx.acquireStatement().dataWriteOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentSchemaStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.FULL );
        SchemaWriteOperations stmt1 = tx.acquireStatement().schemaWriteOperations();

        // when
        ReadOperations stmt2 = tx.acquireStatement().readOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldShareStatementStateForConcurrentSchemaStatementAndSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AccessMode.Static.FULL );
        SchemaWriteOperations stmt1 = tx.acquireStatement().schemaWriteOperations();

        // when
        SchemaWriteOperations stmt2 = tx.acquireStatement().schemaWriteOperations();

        // then
        assertSame( stmt1, stmt2 );
    }

    @Test
    public void shouldNotShareStateForSequentialReadStatementAndReadStatement() throws Exception
    {
        // given
        KernelTransactionFactory.Instances instances =
                KernelTransactionFactory.kernelTransactionWithInternals( AccessMode.Static.READ );
        KernelTransaction tx = instances.transaction;
        Statement statement = tx.acquireStatement();
        ReadOperations ops1 = statement.readOperations();
        verify( instances.storageStatement ).acquire();
        statement.close();

        // when
        verify( instances.storageStatement ).close();
        reset( instances.storageStatement );
        ReadOperations ops2 = tx.acquireStatement().readOperations();

        // then
        verify( instances.storageStatement ).acquire();
    }
}
