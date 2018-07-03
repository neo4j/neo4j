/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.runtime;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.values.virtual.MapValue;

public interface StatementProcessor
{
    StatementMetadata run( String statement, MapValue params ) throws KernelException;

    void streamResult( ThrowingConsumer<BoltResult,Exception> resultConsumer ) throws Exception;

    void reset() throws TransactionFailureException;

    void markCurrentTransactionForTermination();

    boolean hasTransaction();

    boolean hasOpenStatement();

    void validateTransaction() throws KernelException;

    void setQuerySource( BoltQuerySource querySource );

    StatementProcessor EMPTY = new StatementProcessor()
    {
        @Override
        public StatementMetadata run( String statement, MapValue params ) throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to run statements" );
        }

        @Override
        public void streamResult( ThrowingConsumer<BoltResult,Exception> resultConsumer ) throws Exception
        {
            throw new UnsupportedOperationException( "Unable to stream results" );
        }

        @Override
        public void reset() throws TransactionFailureException
        {
        }

        @Override
        public void markCurrentTransactionForTermination()
        {
        }

        @Override
        public boolean hasTransaction()
        {
            return false;
        }

        @Override
        public boolean hasOpenStatement()
        {
            return false;
        }

        @Override
        public void validateTransaction() throws KernelException
        {
        }

        @Override
        public void setQuerySource( BoltQuerySource querySource )
        {
        }
    };
}
