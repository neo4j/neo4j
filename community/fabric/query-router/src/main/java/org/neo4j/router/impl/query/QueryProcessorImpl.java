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

import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;
import static scala.jdk.javaapi.OptionConverters.toJava;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.PreParsedQuery;
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.ast.AdministrationCommand;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.compiler.CypherParsing;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.frontend.phases.BaseState;
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.tracing.CompilationTracer;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.internal.util.RecordingNotificationLogger;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import scala.Option;
import scala.Some$;
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

public class QueryProcessorImpl implements QueryProcessor {

    public static final CatalogName SYSTEM_DATABASE_CATALOG_NAME = CatalogName.of(SYSTEM_DATABASE_NAME);
    private final ProcessedQueryInfoCache cache;
    private final PreParser preParser;
    private final CypherParsing parsing;
    private final CompilationTracer tracer;
    private final CancellationChecker cancellationChecker;
    private final GlobalProcedures globalProcedures;
    private final DatabaseContextProvider<?> databaseContextProvider;
    private final StaticUseEvaluation staticUseEvaluation = new StaticUseEvaluation();

    public QueryProcessorImpl(
            ProcessedQueryInfoCache cache,
            PreParser preParser,
            CypherParsing parsing,
            CompilationTracer tracer,
            CancellationChecker cancellationChecker,
            GlobalProcedures globalProcedures,
            DatabaseContextProvider<?> databaseContextProvider) {
        this.cache = cache;
        this.preParser = preParser;
        this.parsing = parsing;
        this.tracer = tracer;
        this.cancellationChecker = cancellationChecker;
        this.globalProcedures = globalProcedures;
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public ProcessedQueryInfo processQuery(Query query, TargetService targetService, LocationService locationService) {
        var cachedValue = maybeGetFromCache(query, targetService);
        if (cachedValue != null) {
            return cachedValue;
        }

        return doProcessQuery(query, targetService, locationService);
    }

    @Override
    public long clearQueryCachesForDatabase(String databaseName) {
        return cache.clearQueryCachesForDatabase(databaseName);
    }

    private ProcessedQueryInfo maybeGetFromCache(Query query, TargetService targetService) {
        var cachedValue = cache.get(query.text());
        if (cachedValue == null) {
            return null;
        }

        try {
            var databaseReference = targetService.target(cachedValue.catalogInfo());
            // The database and alias info stored in System DB might have changed since
            // the value got cached.
            if (!databaseReference.equals(cachedValue.processedQueryInfo().target())) {
                cache.remove(query.text());
                return null;
            }
        } catch (DatabaseNotFoundException e) {
            // Or the alias is no longer there at all.
            cache.remove(query.text());
            throw e;
        }

        return cachedValue.processedQueryInfo();
    }

    private ProcessedQueryInfo doProcessQuery(
            Query query, TargetService targetService, LocationService locationService) {
        var queryTracer = tracer.compileQuery(query.text());
        var notificationLogger = new RecordingNotificationLogger();
        var preParsedQuery = preParser.preParse(query.text(), notificationLogger);
        var resolver = SignatureResolver.from(globalProcedures.getCurrentView());
        var parsedQuery = parse(query, queryTracer, preParsedQuery, resolver, notificationLogger);
        var catalogInfo = resolveCatalogInfo(parsedQuery.statement());
        var databaseReference = targetService.target(catalogInfo);
        var rewrittenQuery = maybeRewriteQuery(query, parsedQuery, databaseReference);
        var obfuscationMetadata = toJava(parsedQuery.maybeObfuscationMetadata());
        var statementType = StatementType.of(parsedQuery.statement(), resolver);
        var cypherExecutionMode = preParsedQuery.options().queryOptions().executionMode();

        maybePutInTargetDatabaseCache(
                locationService, databaseReference, query, preParsedQuery, parsedQuery, notificationLogger);
        var processedQueryInfo = new ProcessedQueryInfo(
                databaseReference, rewrittenQuery, obfuscationMetadata, statementType, cypherExecutionMode);
        cache.put(query.text(), new ProcessedQueryInfoCache.Value(catalogInfo, processedQueryInfo));
        return processedQueryInfo;
    }

    private TargetService.CatalogInfo resolveCatalogInfo(Statement statement) {
        if (statement instanceof AdministrationCommand) {
            return new TargetService.SingleQueryCatalogInfo(Optional.of(SYSTEM_DATABASE_CATALOG_NAME));
        }

        var graphSelections = staticUseEvaluation.evaluateStaticTopQueriesGraphSelections(statement);
        return toCatalogInfo(graphSelections);
    }

    private Query maybeRewriteQuery(Query query, BaseState parsedQuery, DatabaseReference databaseReference) {
        if (databaseReference instanceof DatabaseReferenceImpl.External externalReference) {
            // TODO: this is where the magic for external references will happen
        }

        return query;
    }

    private void maybePutInTargetDatabaseCache(
            LocationService locationService,
            DatabaseReference databaseReference,
            Query query,
            PreParsedQuery preParsedQuery,
            BaseState parsedQuery,
            RecordingNotificationLogger parsingNotificationLogger) {
        if (locationService.locationOf(databaseReference) instanceof Location.Local localLocation
                // System DB queries are hassle, because they might contain sensitive information
                // and AST cache is not used for them anyway.
                && !localLocation.getDatabaseName().equals(SYSTEM_DATABASE_NAME)) {
            var databaseContext = databaseContextProvider
                    .getDatabaseContext(localLocation.databaseReference().databaseId())
                    .orElseThrow(databaseNotFound(localLocation.getDatabaseName()));

            var resolver = databaseContext.dependencies();
            var queryExecutionEngine = resolver.resolveDependency(ExecutionEngine.class);
            queryExecutionEngine.insertIntoCache(
                    query.text(),
                    preParsedQuery,
                    query.parameters(),
                    parsedQuery,
                    CollectionConverters.asJava(parsingNotificationLogger.notifications()));
        }
    }

    private BaseState parse(
            Query query,
            CompilationTracer.QueryCompilationEvent queryTracer,
            PreParsedQuery preParsedQuery,
            ProcedureSignatureResolver resolver,
            RecordingNotificationLogger notificationLogger) {
        return parsing.parseQuery(
                preParsedQuery.statement(),
                preParsedQuery.rawStatement(),
                notificationLogger,
                preParsedQuery.options().queryOptions().planner().name(),
                Option.apply(preParsedQuery.options().offset()),
                queryTracer,
                query.parameters(),
                cancellationChecker,
                Some$.MODULE$.apply(resolver));
    }

    private TargetService.CatalogInfo toCatalogInfo(Seq<Option<CatalogName>> graphSelections) {
        if (graphSelections.size() == 1) {
            return new TargetService.SingleQueryCatalogInfo(OptionConverters.toJava(graphSelections.head()));
        }

        var catalogNames = CollectionConverters.asJava(graphSelections).stream()
                .map(OptionConverters::toJava)
                .toList();
        return new TargetService.UnionQueryCatalogInfo(catalogNames);
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> new DatabaseNotFoundException("Database " + databaseNameRaw + " not found");
    }
}
