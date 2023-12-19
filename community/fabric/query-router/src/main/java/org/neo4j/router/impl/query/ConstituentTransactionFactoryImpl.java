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

import java.util.function.BiFunction;
import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.rendering.QueryRenderer;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import org.neo4j.router.transaction.DatabaseTransaction;
import org.neo4j.router.transaction.TransactionInfo;
import org.neo4j.values.virtual.MapValue;

public class ConstituentTransactionFactoryImpl implements ConstituentTransactionFactory {

    private final QueryStatementLifecycles statementLifecycles;
    private final CancellationChecker cancellationChecker;
    private final QueryOptions queryOptions;
    private final QueryProcessor queryProcessor;
    private final TransactionInfo transactionInfo;
    private final BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor;
    private final LocationService locationService;
    private final String NL = System.lineSeparator();

    public ConstituentTransactionFactoryImpl(
            QueryProcessor queryProcessor,
            BiFunction<Location, TransactionMode, DatabaseTransaction> transactionFor,
            LocationService locationService,
            QueryStatementLifecycles queryStatementLifecycles,
            TransactionInfo transactionInfo,
            CancellationChecker cancellationChecker,
            QueryOptions queryOptions) {
        this.queryProcessor = queryProcessor;
        this.transactionInfo = transactionInfo;
        this.transactionFor = transactionFor;
        this.locationService = locationService;
        this.statementLifecycles = queryStatementLifecycles;
        this.cancellationChecker = cancellationChecker;
        this.queryOptions = queryOptions;
    }

    @Override
    public ConstituentTransaction transactionFor(DatabaseReference databaseReference) {
        return new ConstituentTransactionImpl(databaseReference);
    }

    public class ConstituentTransactionImpl implements ConstituentTransaction {

        private final DatabaseReference reference;
        private final TargetService targetService;
        private final Location location;

        public ConstituentTransactionImpl(DatabaseReference reference) {
            this.reference = reference;
            this.targetService = new DirectTargetService(reference);
            this.location = locationService.locationOf(reference);
        }

        @Override
        public QueryExecution executeQuery(String queryString, MapValue parameters, QuerySubscriber querySubscriber)
                throws QueryExecutionKernelException {
            var statementLifecycle = statementLifecycles.create(
                    transactionInfo.statementLifecycleTransactionInfo(), queryString, parameters, null);
            statementLifecycle.startProcessing();
            var query = Query.of(QueryRenderer.addOptions(queryString, queryOptions), parameters);
            var processedQuery =
                    queryProcessor.processQuery(query, targetService, (dbRef) -> location, cancellationChecker, false);
            statementLifecycle.doneRouterProcessing(
                    processedQuery.obfuscationMetadata().get(), reference.isComposite());
            TransactionMode mode = TransactionMode.from(
                    transactionInfo.accessMode(),
                    queryOptions.queryOptions().executionMode(),
                    processedQuery.statementType().isReadQuery(),
                    reference.isComposite());
            return transactionFor.apply(location, mode).executeQuery(query, querySubscriber, statementLifecycle);
        }
    }
}
