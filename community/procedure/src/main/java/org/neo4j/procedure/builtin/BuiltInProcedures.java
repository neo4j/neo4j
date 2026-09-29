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
package org.neo4j.procedure.builtin;

import static java.lang.String.format;
import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.kernel.impl.api.TokenAccess.LABELS;
import static org.neo4j.kernel.impl.api.TokenAccess.PROPERTY_KEYS;
import static org.neo4j.kernel.impl.api.TokenAccess.RELATIONSHIP_TYPES;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime;
import static org.neo4j.storageengine.util.StoreIdDecodeUtils.decodeId;

import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.NotThreadSafe;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UnsupportedDatabaseTypes;
import org.neo4j.procedure.memory.ProcedureMemory;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.time.Stopwatch;

@SuppressWarnings({"unused", "WeakerAccess"})
public class BuiltInProcedures {
    private static final int NOT_EXISTING_INDEX_ID = -1;
    static final long LONG_FIELD_NOT_CALCULATED =
            -1; // the user should not even see this because that column should be filtered away (not yielded)

    @Context
    public KernelTransaction kernelTransaction;

    @Context
    public Transaction transaction;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graphDatabaseAPI;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public SpdBuiltInProcedures spdBuiltInProcedures;

    @Context
    public ProcedureMemory procedureMemory;

    @SystemProcedure
    @NotThreadSafe
    @Description("Provides information regarding the database.")
    @Procedure(name = "db.info", mode = READ)
    public Stream<DatabaseInfo> databaseInfo() {
        var storeIdProvider = graphDatabaseAPI.getDependencyResolver().resolveDependency(StoreIdProvider.class);
        var creationTime = formatTime(storeIdProvider.getStoreId().getCreationTime(), getConfiguredTimeZone());
        return Stream.of(new DatabaseInfo(decodeId(storeIdProvider), graphDatabaseAPI.databaseName(), creationTime));
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("List all labels attached to nodes within a database according to the user's access rights. "
            + "The procedure returns empty results if the user is not authorized to view those labels.")
    @Procedure(name = "db.labels", mode = READ)
    public Stream<LabelResult> listLabels() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        AccessMode mode = kernelTransaction.securityContext().mode();
        TokenRead tokenRead = kernelTransaction.tokenRead();

        List<LabelResult> labelsInUse;
        try (KernelTransaction.Revertable ignore = kernelTransaction.overrideWith(SecurityContext.AUTH_DISABLED)) {
            // Get all labels that are in use as seen by a super user
            labelsInUse = stream(LABELS.inUse(
                            kernelTransaction.dataRead(),
                            kernelTransaction.schemaRead(),
                            kernelTransaction.tokenRead()))
                    // filter out labels that are denied or aren't explicitly allowed
                    .filter(label -> mode.allowsTraverseNode(tokenRead.nodeLabel(label.name())))
                    .map(LabelResult::new)
                    .collect(Collectors.toList());
        }
        return labelsInUse.stream();
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("List all property keys in the database.")
    @Procedure(name = "db.propertyKeys", mode = READ)
    public Stream<PropertyKeyResult> listPropertyKeys() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        List<PropertyKeyResult> propertyKeys = stream(PROPERTY_KEYS.all(kernelTransaction.tokenRead()))
                .map(PropertyKeyResult::new)
                .toList();
        return propertyKeys.stream();
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("List all types attached to relationships within a database according to the user's access rights. "
            + "The procedure returns empty results if the user is not authorized to view those relationship types.")
    @Procedure(name = "db.relationshipTypes", mode = READ)
    public Stream<RelationshipTypeResult> listRelationshipTypes() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        AccessMode mode = kernelTransaction.securityContext().mode();
        TokenRead tokenRead = kernelTransaction.tokenRead();
        List<RelationshipTypeResult> relTypesInUse;
        try (KernelTransaction.Revertable ignore = kernelTransaction.overrideWith(SecurityContext.AUTH_DISABLED)) {
            // Get all relTypes that are in use as seen by a super user
            relTypesInUse = stream(RELATIONSHIP_TYPES.inUse(
                            kernelTransaction.dataRead(),
                            kernelTransaction.schemaRead(),
                            kernelTransaction.tokenRead()))
                    // filter out relTypes that are denied or aren't explicitly allowed
                    .filter(type -> mode.allowsTraverseRelType(tokenRead.relationshipType(type.name())))
                    .map(RelationshipTypeResult::new)
                    .collect(Collectors.toList());
        }
        return relTypesInUse.stream();
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("Wait for an index to come online (for example: CALL db.awaitIndex(\"MyIndex\", 300)).")
    @Procedure(name = "db.awaitIndex", mode = READ)
    public void awaitIndex(
            @Name(value = "indexName", description = "The name of the awaited index.") String indexName,
            @Name(value = "timeOutSeconds", defaultValue = "300", description = "The maximum time to wait in seconds.")
                    long timeout)
            throws ProcedureException {
        if (callContext.isSystemDatabase()) {
            return;
        }

        if (spdBuiltInProcedures.isGraphShard()) {
            transaction.schema().awaitIndexOnline(indexName, timeout, TimeUnit.SECONDS);
        } else {
            IndexProcedures indexProcedures = indexProcedures();
            indexProcedures.awaitIndexByName(indexName, timeout, TimeUnit.SECONDS);
        }
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("Wait for all indexes to come online (for example: CALL db.awaitIndexes(300)).")
    @Procedure(name = "db.awaitIndexes", mode = READ)
    public void awaitIndexes(
            @Name(value = "timeOutSeconds", defaultValue = "300", description = "The maximum time to wait in seconds.")
                    long timeout) {
        if (callContext.isSystemDatabase()) {
            return;
        }

        transaction.schema().awaitIndexesOnline(timeout, TimeUnit.SECONDS);
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("Schedule resampling of an index (for example: CALL db.resampleIndex(\"MyIndex\")).")
    @Procedure(name = "db.resampleIndex", mode = READ)
    public void resampleIndex(@Name(value = "indexName", description = "The name of the index.") String indexName)
            throws ProcedureException {
        if (callContext.isSystemDatabase()) {
            return;
        }

        if (spdBuiltInProcedures.isGraphShard()) {
            spdBuiltInProcedures.resampleIndex(indexName);
        }

        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleIndex(indexName);
    }

    @SystemProcedure
    @NotThreadSafe
    @Description("Schedule resampling of all outdated indexes.")
    @Procedure(name = "db.resampleOutdatedIndexes", mode = READ)
    public void resampleOutdatedIndexes() {
        if (callContext.isSystemDatabase()) {
            return;
        }

        if (spdBuiltInProcedures.isGraphShard()) {
            spdBuiltInProcedures.resampleOutdatedIndexes();
        }

        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleOutdatedIndexes();
    }

    @Admin
    @SystemProcedure
    @NotThreadSafe
    @Description(
            "Triggers an index resample and waits for its completion. If the wait time is 0, it skips waiting. After"
                    + " that, the procedure clears query caches. Once the process is complete, queries are planned using"
                    + " the latest database statistics.")
    @Procedure(name = "db.prepareForReplanning", mode = READ)
    public void prepareForReplanning(
            @Name(value = "timeOutSeconds", defaultValue = "300", description = "The maximum time to wait in seconds.")
                    long timeOutSeconds)
            throws ProcedureException {
        if (callContext.isSystemDatabase()) {
            return;
        }

        long remainingTimeOutSeconds = timeOutSeconds;
        if (spdBuiltInProcedures.isGraphShard()) {
            Stopwatch startTime = Stopwatch.start();
            spdBuiltInProcedures.prepareForReplanning(timeOutSeconds);
            remainingTimeOutSeconds = Long.max(0, timeOutSeconds - startTime.elapsed(TimeUnit.SECONDS));
        }

        // Resample indexes
        if (timeOutSeconds != 0 && remainingTimeOutSeconds <= 0) {
            throw new RuntimeException(format(
                    "Could not finish index sampling within the given time limit, %d milliseconds",
                    TimeUnit.SECONDS.toMillis(timeOutSeconds)));
        }
        IndexProcedures indexProcedures = indexProcedures();
        indexProcedures.resampleOutdatedIndexes(remainingTimeOutSeconds);

        // now that index-stats are up-to-date, clear caches so that we are ready to re-plan
        graphDatabaseAPI
                .getDependencyResolver()
                .resolveDependency(QueryExecutionEngine.class)
                .clearQueryCaches();
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(
            name = "db.schema.nodeTypeProperties",
            mode = Mode.READ,
            warning = "The field `propertyTypes` will change output format in the next major version.")
    @Description("Show the derived property schema of the nodes in tabular form.")
    public Stream<NodePropertySchemaInfoResult> nodePropertySchemaCypher5() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        return new SchemaCalculator(kernelTransaction, false).calculateTabularResultStreamForNodes();
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Procedure(name = "db.schema.nodeTypeProperties", mode = Mode.READ)
    @Description("Show the derived property schema of the nodes in tabular form.")
    public Stream<NodePropertySchemaInfoResult> nodePropertySchema() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        if (spdBuiltInProcedures.isGraphShard()) {
            return spdBuiltInProcedures.nodePropertySchema(kernelTransaction);
        }

        return new SchemaCalculator(kernelTransaction, true).calculateTabularResultStreamForNodes();
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Procedure(
            name = "db.schema.relTypeProperties",
            mode = Mode.READ,
            warning = "The field `propertyTypes` will change output format in the next major version.")
    @Description("Show the derived property schema of the relationships in tabular form.")
    public Stream<RelationshipPropertySchemaInfoResult> relationshipPropertySchemaCypher5() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        return new SchemaCalculator(kernelTransaction, false).calculateTabularResultStreamForRels();
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Procedure(name = "db.schema.relTypeProperties", mode = Mode.READ)
    @Description("Show the derived property schema of the relationships in tabular form.")
    public Stream<RelationshipPropertySchemaInfoResult> relationshipPropertySchema() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }

        if (spdBuiltInProcedures.isGraphShard()) {
            return spdBuiltInProcedures.relationshipPropertySchema(kernelTransaction);
        }

        return new SchemaCalculator(kernelTransaction, true).calculateTabularResultStreamForRels();
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_5})
    @Description(
            "Visualizes the schema of the data based on available statistics. A new node is returned for each label."
                    + " The properties represented on the node include: `name` (label name), `indexes` (list of indexes),"
                    + " and `constraints` (list of constraints). A relationship of a given type is returned for all"
                    + " possible combinations of start and end nodes. The properties represented on the relationship"
                    + " include: `name` (type name). Note that this may include additional relationships that do not exist"
                    + " in the data due to the information available in the count store. ")
    @Procedure(name = "db.schema.visualization", mode = READ)
    public Stream<SchemaProcedure.GraphResult> schemaVisualization() {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }
        final var tracker = procedureMemory.newTracker();
        return Stream.of(new SchemaProcedure((InternalTransaction) transaction, procedureMemory, tracker)
                        .buildSchemaGraph(true))
                .onClose(tracker::close);
    }

    @SystemProcedure
    @NotThreadSafe
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description(
            "Visualizes the schema of the data based on available statistics. A new node is returned for each label."
                    + " The properties represented on the node include: `name` (label name), `indexes` (list of indexes),"
                    + " and `constraints` (list of constraints). A relationship of a given type is returned for all"
                    + " possible combinations of start and end nodes. The properties represented on the relationship"
                    + " include: `name` (type name). Note that this may include additional relationships that do not exist"
                    + " in the data due to the information available in the count store. Set the flag to false to get more"
                    + " accurate results. Setting the flag to false could affect performance.")
    @Procedure(name = "db.schema.visualization", mode = READ)
    public Stream<SchemaProcedure.GraphResult> schemaVisualization(
            @Name(
                            value = "estimate",
                            defaultValue = "true",
                            description =
                                    "Toggles a scan of the database for a more correct schema, can affect performance")
                    Boolean estimate) {
        if (callContext.isSystemDatabase()) {
            return Stream.empty();
        }
        if (estimate == null) {
            return Stream.of(new SchemaProcedure.GraphResult(null, null));
        }
        final var tracker = procedureMemory.newTracker();
        return Stream.of(new SchemaProcedure((InternalTransaction) transaction, procedureMemory, tracker)
                        .buildSchemaGraph(estimate))
                .onClose(tracker::close);
    }

    @SystemProcedure(allowExpiredCredentials = true)
    @NotThreadSafe
    @Procedure(name = "db.ping", mode = READ)
    @Description("This procedure can be used by client side tooling to test whether they are correctly connected to a"
            + " database. The procedure is available in all databases and always returns true. A faulty connection"
            + " can be detected by not being able to call this procedure.")
    public Stream<BooleanResult> ping() {
        return Stream.of(new BooleanResult(Boolean.TRUE));
    }

    @Internal
    @NotThreadSafe
    @Procedure(name = "internal.db.system.info", mode = READ)
    @UnsupportedDatabaseTypes(UnsupportedDatabaseTypes.DatabaseType.COMPOSITE)
    public Stream<SysInfoMetricsProvider.SysInfoResult> systemInfo() {
        var sysInfoMetricsProvider =
                graphDatabaseAPI.getDependencyResolver().resolveOptionalDependency(SysInfoMetricsProvider.class);
        if (sysInfoMetricsProvider.isPresent()) {
            return sysInfoMetricsProvider
                    .get()
                    .sysInfoMetrics(callContext.databaseName(), transaction, spdBuiltInProcedures);
        } else {
            return Stream.empty();
        }
    }

    private ZoneId getConfiguredTimeZone() {
        Config config = resolver.resolveDependency(Config.class);
        return config.get(GraphDatabaseSettings.db_timezone).getZoneId();
    }

    private IndexProcedures indexProcedures() {
        return new IndexProcedures(kernelTransaction, resolver.resolveDependency(IndexingService.class));
    }

    private IndexProviderDescriptor getIndexProviderDescriptor(String providerName) {
        return resolver.resolveDependency(IndexingService.class).indexProviderByName(providerName);
    }

    public static class LabelResult {
        @Description("A label within the database.")
        public final String label;

        private LabelResult(String label) {
            this.label = label;
        }

        private LabelResult(Label label) {
            this(label.name());
        }

        public static LabelResult from(String label) {
            return new LabelResult(label);
        }
    }

    public record PropertyKeyResult(
            @Description("A property key in the database.") String propertyKey) {}

    public record DatabaseInfo(
            @Description("The id of the database.") String id,
            @Description("The name of the database.") String name,

            @Description("The creation date of the database, formatted according to the ISO-8601 Standard.")
            String creationDate) {}

    public static class RelationshipTypeResult {
        @Description("A relationship type in the database.")
        public final String relationshipType;

        private RelationshipTypeResult(String relationshipType) {
            this.relationshipType = relationshipType;
        }

        private RelationshipTypeResult(RelationshipType relationshipType) {
            this(relationshipType.name());
        }

        public static RelationshipTypeResult from(String relationshipType) {
            return new RelationshipTypeResult(relationshipType);
        }
    }

    public record BooleanResult(
            @Description("Whether or not the connection call to the database has been successful.")
            Boolean success) {}

    public record NodeResult(Node node) {}

    public record WeightedNodeResult(Node node, double weight) {}

    public record WeightedRelationshipResult(Relationship relationship, double weight) {}

    public record RelationshipResult(Relationship relationship) {}
}
