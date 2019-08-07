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
package org.neo4j.kernel.impl.transaction;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class TransactionMonitorTest
{
    private static Stream<Arguments> parameters()
    {
        return Stream.of(
            arguments( "read", (ThrowingConsumer<GraphDatabaseService, Exception>) db ->
            {
            }, false ),
            arguments( "write", (ThrowingConsumer<GraphDatabaseService, Exception>) GraphDatabaseService::createNode, true )
        );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldCountCommittedTransactions( String name, ThrowingConsumer<GraphDatabaseService, Exception> dbConsumer, boolean isWriteTx ) throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.commit();
            }
            checker.verifyCommitted( isWriteTx, counts );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldCountRolledBackTransactions( String name, ThrowingConsumer<GraphDatabaseService, Exception> dbConsumer, boolean isWriteTx ) throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.rollback();
            }
            checker.verifyRolledBacked( isWriteTx, counts );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "parameters" )
    void shouldCountTerminatedTransactions( String name, ThrowingConsumer<GraphDatabaseService, Exception> dbConsumer, boolean isWriteTx ) throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            TransactionCounters counts = db.getDependencyResolver().resolveDependency( TransactionCounters.class );
            TransactionCountersChecker checker = new TransactionCountersChecker( counts );
            try ( Transaction tx = db.beginTx() )
            {
                dbConsumer.accept( db );
                tx.terminate();
            }
            checker.verifyTerminated( isWriteTx, counts );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
