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
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.PreParsedQuery;
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.QueryOptions;
import org.neo4j.cypher.internal.ast.AdministrationCommand;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.compiler.CypherParsing;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator;
import org.neo4j.cypher.internal.frontend.phases.BaseState;
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver;
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.internal.rewriting.rewriters.RemoveUseRewriter;
import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.tracing.CompilationTracer;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.internal.util.InternalNotification;
import org.neo4j.cypher.internal.util.RecordingNotificationLogger;
import org.neo4j.cypher.rendering.QueryOptionsRenderer;
import org.neo4j.cypher.rendering.QueryRenderer;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.router.QueryRouterException;
import org.neo4j.router.location.LocationService;
import org.neo4j.router.query.Query;
import org.neo4j.router.query.QueryProcessor;
import org.neo4j.router.query.TargetService;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
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
    private final GlobalProcedures globalProcedures;
    private final DatabaseContextProvider<?> databaseContextProvider;
    private final StaticUseEvaluation staticUseEvaluation = new StaticUseEvaluation();

    public QueryProcessorImpl(
            ProcessedQueryInfoCache cache,
            PreParser preParser,
            CypherParsing parsing,
            CompilationTracer tracer,
            GlobalProcedures globalProcedures,
            DatabaseContextProvider<?> databaseContextProvider) {
        this.cache = cache;
        this.preParser = preParser;
        this.parsing = parsing;
        this.tracer = tracer;
        this.globalProcedures = globalProcedures;
        this.databaseContextProvider = databaseContextProvider;
    }

    @Override
    public ProcessedQueryInfo processQuery(
            Query query,
            TargetService targetService,
            LocationService locationService,
            CancellationChecker cancellationChecker,
            boolean targetsComposite,
            String sessionDatabase) {

        var cachedValue = getFromCache(query, cancellationChecker, targetsComposite, sessionDatabase);

        var databaseReference = targetService.target(cachedValue.catalogInfo());

        maybePutInTargetDatabaseCache(
                locationService,
                databaseReference,
                query,
                cachedValue.preParsedQuery(),
                cachedValue.parsedQuery(),
                cachedValue.parsingNotifications());

        var rewrittenQuery = query;
        if (shouldRewriteQuery(databaseReference)) {
            rewrittenQuery = Query.of(
                    cachedValue.rewrittenQueryText(),
                    query.parameters().updatedWith(cachedValue.maybeExtractedParams()));
        }

        return new ProcessedQueryInfo(
                databaseReference,
                rewrittenQuery,
                toJava(cachedValue.parsedQuery().maybeObfuscationMetadata()),
                cachedValue.statementType(),
                cachedValue.preParsedQuery().options());
    }

    @Override
    public long clearQueryCachesForDatabase(String databaseName) {
        return cache.clearQueryCachesForDatabase(databaseName);
    }

    @Override
    public DatabaseContextProvider<?> databaseContextProvider() {
        return this.databaseContextProvider;
    }

    private ProcessedQueryInfoCache.Value getFromCache(
            Query query, CancellationChecker cancellationChecker, boolean targetsComposite, String sessionDatabase) {
        var notificationLogger = new RecordingNotificationLogger();
        var preParsedQuery = preParser.preParse(query.text(), notificationLogger);

        var cachedValue = cache.get(preParsedQuery, query.parameters());

        if (cachedValue == null) {
            var preparedForCacheQuery = prepareQueryForCache(
                    preParsedQuery, notificationLogger, query, cancellationChecker, targetsComposite, sessionDatabase);
            cache.put(preParsedQuery, query.parameters(), preparedForCacheQuery);
            cachedValue = preparedForCacheQuery;
        }
        return cachedValue;
    }

    private ProcessedQueryInfoCache.Value prepareQueryForCache(
            PreParsedQuery preParsedQuery,
            RecordingNotificationLogger notificationLogger,
            Query query,
            CancellationChecker cancellationChecker,
            boolean targetsComposite,
            String sessionDatabase) {
        var queryTracer = tracer.compileQuery(query.text());
        var resolver = SignatureResolver.from(globalProcedures.getCurrentView());
        var parsedQuery = parse(
                query,
                queryTracer,
                preParsedQuery,
                resolver,
                notificationLogger,
                cancellationChecker,
                targetsComposite,
                sessionDatabase);
        var catalogInfo = resolveCatalogInfo(parsedQuery.statement(), targetsComposite);
        var rewrittenQueryText = rewriteQueryText(parsedQuery, preParsedQuery.options(), cancellationChecker);
        var maybeExtractedParams = formatMaybeExtractedParams(parsedQuery);
        var statementType = StatementType.of(parsedQuery.statement(), resolver);
        var parsingNotifications = CollectionConverters.asJava(notificationLogger.notifications());

        return new ProcessedQueryInfoCache.Value(
                catalogInfo,
                rewrittenQueryText,
                maybeExtractedParams,
                preParsedQuery,
                parsedQuery,
                statementType,
                parsingNotifications);
    }

    private TargetService.CatalogInfo resolveCatalogInfo(Statement statement, boolean targetsComposite) {
        if (statement instanceof AdministrationCommand) {
            return new TargetService.SingleQueryCatalogInfo(Optional.of(SYSTEM_DATABASE_CATALOG_NAME));
        }

        if (targetsComposite) {
            return new TargetService.CompositeCatalogInfo();
        }

        var graphSelections = staticUseEvaluation.evaluateStaticTopQueriesGraphSelections(statement);
        return toCatalogInfo(graphSelections);
    }

    private static String rewriteQueryText(
            BaseState parsedQuery, QueryOptions queryOptions, CancellationChecker cancellationChecker) {
        var rewrittenStatement = flattenBooleanOperators
                .instance(cancellationChecker)
                .apply(RemoveUseRewriter.instance().apply(parsedQuery.statement()));
        var rewrittenStatementString = QueryRenderer.render((Statement) rewrittenStatement);

        return QueryOptionsRenderer.addOptions(rewrittenStatementString, queryOptions);
    }

    private static MapValue formatMaybeExtractedParams(BaseState parsedQuery) {
        var mapValueBuilder = new MapValueBuilder();
        var extractedParams = parsedQuery.maybeExtractedParams().get();
        if (extractedParams.nonEmpty()) {
            var evaluator = new SimpleInternalExpressionEvaluator();
            extractedParams.foreach(param -> mapValueBuilder.add(
                    param._1.name(), evaluator.evaluate(param._2, MapValue.EMPTY, CypherRow.empty())));
        }
        return mapValueBuilder.build();
    }

    private static Boolean shouldRewriteQuery(DatabaseReference databaseReference) {
        return (databaseReference instanceof DatabaseReferenceImpl.External);
    }

    private void maybePutInTargetDatabaseCache(
            LocationService locationService,
            DatabaseReference databaseReference,
            Query query,
            PreParsedQuery preParsedQuery,
            BaseState parsedQuery,
            Set<InternalNotification> parsingNotifications) {
        Location location;
        try {
            location = locationService.locationOf(databaseReference);
        } catch (Exception e) {
            // Let's ignore any routing-related errors while trying to insert
            // into a cache. This might not be the biggest problem the query has
            // and if yes, we will get the error again later.
            return;
        }

        if (location instanceof Location.Local localLocation
                // System DB queries are hassle, because they might contain sensitive information
                // and AST cache is not used for them anyway.
                && !localLocation.getDatabaseName().equals(SYSTEM_DATABASE_NAME)) {
            var databaseContext = databaseContextProvider
                    .getDatabaseContext(localLocation.databaseReference().databaseId())
                    .orElseThrow(databaseNotFound(localLocation.getDatabaseName()));
            checkDatabaseAvailable(databaseContext);

            var resolver = databaseContext.dependencies();
            var queryExecutionEngine = resolver.resolveDependency(ExecutionEngine.class);
            queryExecutionEngine.insertIntoCache(
                    query.text(), preParsedQuery, query.parameters(), parsedQuery, parsingNotifications);
        }
    }

    private void checkDatabaseAvailable(DatabaseContext databaseContext) {
        try {
            databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        } catch (UnavailableException e) {
            throw new QueryRouterException(e.status(), e);
        }
    }

    private BaseState parse(
            Query query,
            CompilationTracer.QueryCompilationEvent queryTracer,
            PreParsedQuery preParsedQuery,
            ProcedureSignatureResolver resolver,
            RecordingNotificationLogger notificationLogger,
            CancellationChecker cancellationChecker,
            boolean targetsComposite,
            String sessionDatabase) {
        return parsing.parseQuery(
                preParsedQuery.statement(),
                preParsedQuery.rawStatement(),
                notificationLogger,
                preParsedQuery.options().queryOptions().planner().name(),
                Option.apply(preParsedQuery.options().offset()),
                queryTracer,
                query.parameters(),
                cancellationChecker,
                Some$.MODULE$.apply(resolver),
                targetsComposite,
                sessionDatabase);
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
