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
import static scala.jdk.CollectionConverters.CollectionHasAsScala;
import static scala.jdk.javaapi.OptionConverters.toJava;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.ast.CatalogName;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.compiler.CypherParsing;
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver;
import org.neo4j.cypher.internal.evaluator.SimpleInternalExpressionEvaluator;
import org.neo4j.cypher.internal.frontend.phases.BaseState;
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage;
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver;
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators;
import org.neo4j.cypher.internal.javacompat.InternalQueryExecutionEngine;
import org.neo4j.cypher.internal.notification.InternalNotification;
import org.neo4j.cypher.internal.notification.RecordingNotificationLogger;
import org.neo4j.cypher.internal.preparser.PreParsedQuery;
import org.neo4j.cypher.internal.preparser.QueryOptions;
import org.neo4j.cypher.internal.rewriting.rewriters.RemoveUseRewriter;
import org.neo4j.cypher.internal.runtime.CypherRow;
import org.neo4j.cypher.internal.spi.ExceptionTranslatingResolver;
import org.neo4j.cypher.internal.tracing.CompilationTracer;
import org.neo4j.cypher.internal.util.CancellationChecker;
import org.neo4j.cypher.rendering.QueryOptionsRenderer;
import org.neo4j.cypher.rendering.QueryRenderer;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.api.DatabaseNotFoundHelper;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.QueryStatementLifecycles;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdRepository;
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
import scala.collection.immutable.Seq;
import scala.jdk.javaapi.CollectionConverters;
import scala.jdk.javaapi.OptionConverters;

public class QueryProcessorImpl implements QueryProcessor {
    // the system database resolves to the same database in both strict and non-strict mode
    public static final CatalogName SYSTEM_DATABASE_CATALOG_NAME = CatalogName.of(SYSTEM_DATABASE_NAME, true);
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
    public PreParsedQuery preParse(Query query, CypherVersion defaultLanguage) {
        return preParser.preParse(query.text(), defaultLanguage);
    }

    @Override
    public ProcessedQueryInfo processQuery(
            Query query,
            PreParsedQuery preParsedQuery,
            TargetService targetService,
            LocationService locationService,
            CancellationChecker cancellationChecker,
            DatabaseReference sessionDatabase,
            QueryStatementLifecycles.StatementLifecycle statementLifecycle) {

        var cachedValue = getFromCache(query, preParsedQuery, cancellationChecker, sessionDatabase, statementLifecycle);

        QueryTarget queryTarget = targetService.target(cachedValue.catalogInfo());

        maybePutInTargetDatabaseCache(
                locationService,
                queryTarget.reference(),
                query,
                cachedValue.preParsedQuery(),
                cachedValue.parsedQuery(),
                cachedValue.parsingNotifications());

        var rewrittenQuery = query;
        if (shouldRewriteQuery(queryTarget.reference())) {
            rewrittenQuery = Query.of(
                    cachedValue.rewrittenQueryText(),
                    query.parameters().updatedWith(cachedValue.maybeExtractedParams()));
        }

        return new ProcessedQueryInfo(
                queryTarget.reference(),
                rewrittenQuery,
                toJava(cachedValue.parsedQuery().maybeObfuscationMetadata()),
                cachedValue.statementType(),
                cachedValue.preParsedQuery().options(),
                cachedValue.parsingNotifications(),
                queryTarget.routingNotifications());
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
            Query query,
            PreParsedQuery preParsedQuery,
            CancellationChecker cancellationChecker,
            DatabaseReference sessionDatabase,
            QueryStatementLifecycles.StatementLifecycle statementLifecycle) {
        var notificationLogger = new RecordingNotificationLogger();
        var shadowedFunctions = globalProcedures
                .getCurrentView()
                .getAllShadowedNames(QueryLanguage.toKernelScope(preParsedQuery.resolvedLanguage()));
        var cachedValue = cache.get(preParsedQuery, query.parameters());

        if (cachedValue == null) {
            var preparedForCacheQuery = prepareQueryForCache(
                    preParsedQuery,
                    notificationLogger,
                    query,
                    cancellationChecker,
                    sessionDatabase,
                    shadowedFunctions,
                    statementLifecycle);
            if (preparedForCacheQuery.catalogInfo().canBeCached()) {
                cache.put(preParsedQuery, query.parameters(), preparedForCacheQuery);
            }
            cachedValue = preparedForCacheQuery;
        }
        return cachedValue;
    }

    private ProcessedQueryInfoCache.Value prepareQueryForCache(
            PreParsedQuery preParsedQuery,
            RecordingNotificationLogger notificationLogger,
            Query query,
            CancellationChecker cancellationChecker,
            DatabaseReference sessionDatabase,
            Set<String> shadowedFunctions,
            QueryStatementLifecycles.StatementLifecycle statementLifecycle) {
        var queryTracer = tracer.compileQuery(query.text());
        var resolver = new ExceptionTranslatingResolver(
                SignatureResolver.from(globalProcedures.getCurrentView(), preParsedQuery.resolvedLanguage()));
        var parsedQuery = parse(
                query,
                queryTracer,
                preParsedQuery,
                resolver,
                notificationLogger,
                cancellationChecker,
                sessionDatabase,
                shadowedFunctions,
                statementLifecycle);
        var statementType = StatementType.of(parsedQuery.statement(), resolver);
        var catalogInfo = resolveCatalogInfo(
                statementType,
                parsedQuery.statement(),
                sessionDatabase.isComposite(),
                databaseContextProvider.databaseIdRepository(),
                query);

        var rewrittenQueryText = rewriteQueryText(parsedQuery, preParsedQuery.options(), cancellationChecker);
        var maybeExtractedParams = formatMaybeExtractedParams(parsedQuery);
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

    private TargetService.CatalogInfo resolveCatalogInfo(
            StatementType statementType,
            Statement statement,
            boolean targetsComposite,
            DatabaseIdRepository databaseIdRepository,
            Query query) {
        if (statementType.statementType().equals(StatementType.AdminCommand())) {
            return new TargetService.SingleQueryCatalogInfo(Optional.of(SYSTEM_DATABASE_CATALOG_NAME), true);
        }

        if (targetsComposite) {
            return new TargetService.CompositeCatalogInfo();
        }

        var graphSelections = staticUseEvaluation.evaluateStaticTopQueriesGraphSelections(
                statement, databaseIdRepository, query.parameters());
        return toCatalogInfo(graphSelections);
    }

    private static String rewriteQueryText(
            BaseState parsedQuery, QueryOptions queryOptions, CancellationChecker cancellationChecker) {
        var rewrittenStatement = flattenBooleanOperators
                .instance(cancellationChecker)
                .apply(RemoveUseRewriter.instance().apply(parsedQuery.statement()));

        var rewrittenStatementString = QueryRenderer.render((Statement) rewrittenStatement);

        return QueryOptionsRenderer.addOptions(
                rewrittenStatementString, queryOptions.withQueryLanguage(queryOptions.resolvedLanguage()));
    }

    private static MapValue formatMaybeExtractedParams(BaseState parsedQuery) {
        if (parsedQuery.maybeExtractedParams().isEmpty()) {
            return MapValue.EMPTY;
        }
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
            var queryExecutionEngine = resolver.resolveDependency(InternalQueryExecutionEngine.class);
            queryExecutionEngine.insertIntoCache(
                    query.text(), preParsedQuery, query.parameters(), parsedQuery, parsingNotifications);
        }
    }

    private void checkDatabaseAvailable(DatabaseContext databaseContext) {
        try {
            databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        } catch (UnavailableException e) {
            throw QueryRouterException.wrapError(e);
        }
    }

    private BaseState parse(
            Query query,
            CompilationTracer.QueryCompilationEvent queryTracer,
            PreParsedQuery preParsedQuery,
            ScopedProcedureSignatureResolver resolver,
            RecordingNotificationLogger notificationLogger,
            CancellationChecker cancellationChecker,
            DatabaseReference sessionDatabase,
            Set<String> shadowedFunctions,
            QueryStatementLifecycles.StatementLifecycle statementLifecycle) {
        // Wire the obfuscator onto the ExecutingQuery as soon as obfuscation metadata is collected.
        return parsing.parseQueryWithObfuscatorCallback(
                preParsedQuery.statement(),
                preParsedQuery.rawStatement(),
                preParsedQuery.resolvedLanguage(),
                notificationLogger,
                preParsedQuery.options().queryOptions().planner().name(),
                Option.apply(preParsedQuery.options().offset()),
                queryTracer,
                query.parameters(),
                cancellationChecker,
                resolver,
                sessionDatabase,
                preParsedQuery.options().queryOptions().planMode().isScope(),
                CollectionHasAsScala(shadowedFunctions).asScala().toSet(),
                metadata -> statementLifecycle.onObfuscatorReady(
                        metadata, preParsedQuery.options().offset()));
    }

    private TargetService.CatalogInfo toCatalogInfo(Seq<Option<StaticUseEvaluation.CatalogInfo>> graphSelections) {
        if (graphSelections.size() == 1) {
            var catalogInfo = graphSelections.head();
            var canBeCached = catalogInfo.isEmpty() || catalogInfo.get().canBeCached();
            var catalogName = OptionConverters.toJava(catalogInfo).map(info -> info.catalogName());
            return new TargetService.SingleQueryCatalogInfo(catalogName, canBeCached);
        }

        final boolean[] canBeCached = {true};
        var catalogNames = CollectionConverters.asJava(graphSelections).stream()
                .map(OptionConverters::toJava)
                .map(catalogInfoOption -> catalogInfoOption.map(catalogInfo -> {
                    if (!catalogInfo.canBeCached()) {
                        canBeCached[0] = false;
                    }
                    return catalogInfo.catalogName();
                }))
                .toList();

        return new TargetService.UnionQueryCatalogInfo(catalogNames, canBeCached[0]);
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound(String databaseNameRaw) {
        return () -> DatabaseNotFoundHelper.databaseNameNotFoundWithoutDot((databaseNameRaw));
    }
}
