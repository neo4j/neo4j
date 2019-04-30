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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.security.AnonymousContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.api.KernelTransactionFactory.kernelTransaction;

class TransactionStatementSequenceTest
{
    @Test
    void shouldAllowReadStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.read() );
        tx.dataRead();

        // when / then
        tx.dataRead();
    }

    @Test
    void shouldAllowDataStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.dataRead();

        // when / then
        tx.dataWrite();
    }

    @Test
    void shouldAllowSchemaStatementAfterReadStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.dataRead();

        // when / then
        tx.schemaWrite();
    }

    @Test
    void shouldRejectSchemaStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.dataWrite();

        // when
        InvalidTransactionTypeKernelException exception = assertThrows( InvalidTransactionTypeKernelException.class, tx::schemaWrite );
        assertEquals( "Cannot perform schema updates in a transaction that has performed data updates.", exception.getMessage() );
    }

    @Test
    void shouldRejectDataStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.schemaWrite();

        // when
        InvalidTransactionTypeKernelException exception = assertThrows( InvalidTransactionTypeKernelException.class, tx::dataWrite );
        assertEquals( "Cannot perform data updates in a transaction that has performed schema updates.", exception.getMessage() );
    }

    @Test
    void shouldAllowDataStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.dataWrite();

        // when / then
        tx.dataWrite();
    }

    @Test
    void shouldAllowSchemaStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.schemaWrite();

        // when / then
        tx.schemaWrite();
    }

    @Test
    void shouldAllowReadStatementAfterDataStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AnonymousContext.write() );
        tx.dataWrite();

        // when / then
        tx.dataRead();
    }

    @Test
    void shouldAllowReadStatementAfterSchemaStatement() throws Exception
    {
        // given
        KernelTransaction tx = kernelTransaction( AUTH_DISABLED );
        tx.schemaWrite();

        // when / then
        tx.dataRead();
    }
}
