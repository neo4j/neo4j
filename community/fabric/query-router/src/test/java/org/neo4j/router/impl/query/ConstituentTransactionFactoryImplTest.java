/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.router.impl.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.cypher.internal.options.CypherQueryOptions;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.internal.util.InputPosition;
import org.neo4j.cypher.internal.util.ObfuscationMetadata;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.values.virtual.MapValue;
import scala.collection.immutable.HashSet;

class ConstituentTransactionFactoryImplTest {
    private final String NL = System.lineSeparator();

    @Test
    void testWithoutPreParserOptions() throws QueryExecutionKernelException {
        CypherQueryOptions queryOptions = CypherQueryOptions.defaultOptions();
        DatabaseTransaction innerTransaction = mock(DatabaseTransaction.class);
        BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor =
                (location, transactionMode) -> innerTransaction;
        var constituentTransactionFactory = getConstituentTransactionFactory(queryOptions, transactionFor);

        var transaction = constituentTransactionFactory.transactionFor(mock(DatabaseReference.class));
        transaction.executeQuery("MATCH (n) RETURN n", MapValue.EMPTY, QuerySubscriber.DO_NOTHING_SUBSCRIBER);

        var query = Query.of("MATCH (n) RETURN n");
        verify(innerTransaction, times(1)).executeQuery(eq(query), any(), any());
    }

    @Test
    void testWithSinglePreParserOptions() throws QueryExecutionKernelException {
        CypherQueryOptions queryOptions = CypherQueryOptions.fromValues(
                CypherConfiguration.fromConfig(Config.newBuilder()
                        .set(
                                GraphDatabaseInternalSettings.cypher_runtime,
                                GraphDatabaseInternalSettings.CypherRuntime.INTERPRETED)
                        .build()),
                new HashSet<>());
        DatabaseTransaction innerTransaction = mock(DatabaseTransaction.class);
        BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor =
                (location, transactionMode) -> innerTransaction;
        var constituentTransactionFactory = getConstituentTransactionFactory(queryOptions, transactionFor);

        var transaction = constituentTransactionFactory.transactionFor(mock(DatabaseReference.class));
        transaction.executeQuery("MATCH (n) RETURN n", MapValue.EMPTY, QuerySubscriber.DO_NOTHING_SUBSCRIBER);

        var query = Query.of("CYPHER runtime=interpreted" + NL + "MATCH (n) RETURN n");
        verify(innerTransaction, times(1)).executeQuery(eq(query), any(), any());
    }

    @Test
    void testWithMultiplePreParserOptions() throws QueryExecutionKernelException {
        CypherQueryOptions queryOptions = CypherQueryOptions.fromValues(
                CypherConfiguration.fromConfig(Config.newBuilder()
                        .set(
                                GraphDatabaseInternalSettings.cypher_runtime,
                                GraphDatabaseInternalSettings.CypherRuntime.INTERPRETED)
                        .build()),
                asScala(Map.of("PLANNER", "dp", "debug", "toString", "DEbug", "ast"))
                        .toSet());
        DatabaseTransaction innerTransaction = mock(DatabaseTransaction.class);
        BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor =
                (location, transactionMode) -> innerTransaction;
        var constituentTransactionFactory = getConstituentTransactionFactory(queryOptions, transactionFor);

        var transaction = constituentTransactionFactory.transactionFor(mock(DatabaseReference.class));
        transaction.executeQuery("MATCH (n) RETURN n", MapValue.EMPTY, QuerySubscriber.DO_NOTHING_SUBSCRIBER);

        var query =
                Query.of("CYPHER planner=dp runtime=interpreted debug=ast debug=tostring" + NL + "MATCH (n) RETURN n");
        verify(innerTransaction, times(1)).executeQuery(eq(query), any(), any());
    }

    private ConstituentTransactionFactory getConstituentTransactionFactory(
            CypherQueryOptions cypherQueryOptions,
            BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor) {
        QueryProcessor queryProcessor = mock(QueryProcessor.class);
        QueryOptions queryOptions = QueryOptions.apply(InputPosition.NONE(), cypherQueryOptions, false, false);
        StatementType statementType = StatementType.of(StatementType.Query());
        QueryProcessor.ProcessedQueryInfo processedQueryInfo = mock(QueryProcessor.ProcessedQueryInfo.class);
        when(queryProcessor.processQuery(any(), any(), any(), any(), anyBoolean()))
                .thenReturn(processedQueryInfo);
        when(processedQueryInfo.obfuscationMetadata()).thenReturn(Optional.of(ObfuscationMetadata.empty()));
        when(processedQueryInfo.statementType()).thenReturn(statementType);

        LocationService locationService = (databaseReference) -> mock(Location.Local.class);
        TransactionInfo transactionInfo = mock(TransactionInfo.class);
        QueryStatementLifecycles queryStatementLifecycles = mock(QueryStatementLifecycles.class);
        when(queryStatementLifecycles.create(any(), anyString(), any(), any()))
                .thenReturn(mock(QueryStatementLifecycles.StatementLifecycle.class));
        return new ConstituentTransactionFactoryImpl(
                queryProcessor,
                transactionFor,
                locationService,
                queryStatementLifecycles,
                transactionInfo,
                CancellationChecker.neverCancelled(),
                queryOptions);
    }
}
