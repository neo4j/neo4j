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

import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.rendering.QueryOptionsRenderer;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.kernel.impl.query.QueryExecution;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.QuerySubscriber;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import org.neo4j.router.transaction.RouterTransactionContext;
import org.neo4j.router.util.Errors;
import org.neo4j.values.virtual.MapValue;

public class ConstituentTransactionFactoryImpl implements ConstituentTransactionFactory {

    private final QueryStatementLifecycles statementLifecycles;
    private final CancellationChecker cancellationChecker;
    private final QueryOptions queryOptions;
    private final RouterTransactionContext context;
    private final QueryProcessor queryProcessor;

    public ConstituentTransactionFactoryImpl(
            QueryProcessor queryProcessor,
            QueryStatementLifecycles queryStatementLifecycles,
            CancellationChecker cancellationChecker,
            QueryOptions queryOptions,
            RouterTransactionContext context) {
        this.queryProcessor = queryProcessor;
        this.statementLifecycles = queryStatementLifecycles;
        this.cancellationChecker = cancellationChecker;
        this.queryOptions = queryOptions;
        this.context = context;
    }

    @Override
    public ConstituentTransaction transactionFor(DatabaseReference databaseReference) {
        return new ConstituentTransactionImpl(databaseReference);
    }

    @Override
    public DatabaseReferenceImpl.Composite sessionDatabase() {
        return (DatabaseReferenceImpl.Composite) context.sessionDatabaseReference();
    }

    public class ConstituentTransactionImpl implements ConstituentTransaction {

        private final DatabaseReference targetReference;
        private final TargetService targetService;
        private final Location location;

        public ConstituentTransactionImpl(DatabaseReference targetReference) {
            if (sessionDatabase()
                    .getConstituentByName(targetReference.fullName().name())
                    .isEmpty()) {
                // We should not end up here. This should be taken care of by semantic analysis or dynamic graph
                // functions.
                Errors.cantAccessOutsideCompositeMessage(targetReference, sessionDatabase());
            }
            this.targetReference = targetReference;
            this.targetService = new DirectTargetService(targetReference);
            this.location = context.locationService().locationOf(targetReference);
        }

        @Override
        public QueryExecution executeQuery(String queryString, MapValue parameters, QuerySubscriber querySubscriber)
                throws QueryExecutionKernelException {
            var statementLifecycle = statementLifecycles.create(
                    context.transactionInfo().statementLifecycleTransactionInfo(), queryString, parameters, null);
            statementLifecycle.startProcessing();
            var query = Query.of(QueryOptionsRenderer.addOptions(queryString, queryOptions), parameters);
            var processedQuery = queryProcessor.processQuery(
                    // the session database can be ignored in the constituent for now
                    query, targetService, (dbRef) -> location, cancellationChecker, false, null);
            statementLifecycle.doneRouterProcessing(
                    processedQuery.obfuscationMetadata().get(),
                    processedQuery.queryOptions().offset().offset(),
                    targetReference.isComposite());
            TransactionMode mode = TransactionMode.from(
                    context.transactionInfo().accessMode(),
                    queryOptions.queryOptions().executionMode(),
                    processedQuery.statementType().isReadQuery(),
                    targetReference.isComposite());
            return context.transactionFor(location, mode).executeQuery(query, querySubscriber, statementLifecycle);
        }
    }
}
