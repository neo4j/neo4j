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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.kernel.api.KernelTransactionFactory.kernelTransaction;

public class TransactionStatementSequenceTest
{
    @Test
    public void shouldAllowReadStatementAfterReadStatement()
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.read() );
        tx.acquireStatement().readOperations();

        // when / then
        tx.acquireStatement().readOperations();
    }

    @Test
    public void shouldAllowDataStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.acquireStatement().readOperations();

        // when / then
        tx.acquireStatement().dataWriteOperations();
    }

    @Test
    public void shouldAllowSchemaStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.acquireStatement().readOperations();

        // when / then
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldRejectSchemaStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.acquireStatement().dataWriteOperations();

        // when
        try
        {
            tx.acquireStatement().schemaWriteOperations();

            fail( "expected exception" );
        }
        // then
        catch ( InvalidTransactionTypeKernelException e )
        {
            assertEquals( "Cannot perform schema updates in a transaction that has performed data updates.",
                          e.getMessage() );
        }
    }

    @Test
    public void shouldRejectDataStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.acquireStatement().schemaWriteOperations();

        // when
        try
        {
            tx.acquireStatement().dataWriteOperations();

            fail( "expected exception" );
        }
        // then
        catch ( InvalidTransactionTypeKernelException e )
        {
            assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.",
                          e.getMessage() );
        }
    }

    @Test
    public void shouldAllowDataStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.acquireStatement().dataWriteOperations();

        // when / then
        tx.acquireStatement().dataWriteOperations();
    }

    @Test
    public void shouldAllowSchemaStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.acquireStatement().schemaWriteOperations();

        // when / then
        tx.acquireStatement().schemaWriteOperations();
    }

    @Test
    public void shouldAllowReadStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.acquireStatement().dataWriteOperations();

        // when / then
        tx.acquireStatement().readOperations();
    }

    @Test
    public void shouldAllowReadStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.acquireStatement().schemaWriteOperations();

        // when / then
        tx.acquireStatement().readOperations();
    }
}
