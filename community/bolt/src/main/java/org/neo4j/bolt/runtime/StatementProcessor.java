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
package org.neo4j.bolt.runtime;

import java.time.Duration;
import java.util.Map;

import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v4.messaging.ResultConsumer;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.values.virtual.MapValue;

public interface StatementProcessor
{
    void beginTransaction( Bookmark bookmark ) throws KernelException;

    void beginTransaction( Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetadata ) throws KernelException;

    StatementMetadata run( String statement, MapValue params ) throws KernelException;

    StatementMetadata run( String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetaData ) throws KernelException;

    Bookmark streamResult( int statementId, ResultConsumer resultConsumer ) throws Throwable;

    Bookmark commitTransaction() throws KernelException;

    void rollbackTransaction() throws KernelException;

    void reset() throws TransactionFailureException;

    void markCurrentTransactionForTermination();

    boolean hasTransaction();

    boolean hasOpenStatement();

    Status validateTransaction() throws KernelException;

    DatabaseId databaseId();

    StatementProcessor EMPTY = new StatementProcessor()
    {
        @Override
        public void beginTransaction( Bookmark bookmark ) throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to run statements" );
        }

        @Override
        public void beginTransaction( Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetadata ) throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to begin a transaction" );
        }

        @Override
        public StatementMetadata run( String statement, MapValue params ) throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to run statements" );
        }

        @Override
        public StatementMetadata run( String statement, MapValue params, Bookmark bookmark, Duration txTimeout, Map<String,Object> txMetaData )
                throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to run statements" );
        }

        @Override
        public Bookmark streamResult( int statementId, ResultConsumer resultConsumer )
        {
            throw new UnsupportedOperationException( "Unable to stream results" );
        }

        @Override
        public Bookmark commitTransaction() throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to commit a transaction" );
        }

        @Override
        public void rollbackTransaction() throws KernelException
        {
            throw new UnsupportedOperationException( "Unable to rollback a transaction" );
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
        public Status validateTransaction() throws KernelException
        {
            return null;
        }

        @Override
        public DatabaseId databaseId()
        {
            throw new UnsupportedOperationException( "Unable to read the database ID" );
        }

        @Override
        public String toString()
        {
            return "Empty Statement Processor";
        }
    };
}
