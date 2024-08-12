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
import static java.util.Collections.singletonList;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.upgrade_procedure_wait_timeout;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.UNINITIALIZED;
import static org.neo4j.kernel.api.exceptions.Status.Procedure.ProcedureCallFailed;
import static org.neo4j.procedure.Mode.DBMS;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;
import static org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime;
import static org.neo4j.storageengine.util.StoreIdDecodeUtils.decodeId;

import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.capabilities.CapabilitiesService;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphComponents.UpgradeChecker;
import org.neo4j.fabric.executor.FabricExecutor;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.api.procedure.SystemProcedure;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Internal;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.router.QueryRouter;
import org.neo4j.router.transaction.TransactionLookup;
import org.neo4j.storageengine.api.StoreIdProvider;

@SuppressWarnings("unused")
public class BuiltInDbmsProcedures {
    /**
     * Upgrade result message when explicit upgrade procedures are waiting for automatic upgrade to pass.
     */
    public static final String UPGRADE_PENDING_RESULT = "Upgrade pending";

    private static final int HARD_CHAR_LIMIT = 2048;

    @Context
    public Log log;

    @Context
    public DependencyResolver resolver;

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public Transaction transaction;

    @Context
    public KernelTransaction kernelTransaction;

    @Context
    public SecurityContext securityContext;

    @Context
    public ProcedureCallContext callContext;

    @Context
    public SystemGraphComponents systemGraphComponents;

    @SystemProcedure
    @Description("Provides information regarding the DBMS.")
    @Procedure(name = "dbms.info", mode = DBMS)
    public Stream<SystemInfo> databaseInfo() throws NoSuchAlgorithmException {
        var systemGraph = getSystemDatabase();
        return dbmsInfo(systemGraph);
    }

    public static Stream<SystemInfo> dbmsInfo(GraphDatabaseAPI system) {
        Config config = system.getDependencyResolver().resolveDependency(Config.class);
        var storeIdProvider = getSystemDatabaseStoreIdProvider(system);
        var creationTime = formatTime(
                storeIdProvider.getStoreId().getCreationTime(),
                config.get(GraphDatabaseSettings.db_timezone).getZoneId());
        return Stream.of(new SystemInfo(decodeId(storeIdProvider), system.databaseName(), creationTime));
    }

    @Admin
    @SystemProcedure
    @Description("List the currently active configuration settings of Neo4j.")
    @Procedure(name = "dbms.listConfig", mode = DBMS)
    public Stream<ConfigResult> listConfig(@Name(value = "searchString", defaultValue = "") String searchString) {
        String lowerCasedSearchString = searchString.toLowerCase();
        List<ConfigResult> results = new ArrayList<>();

        Config config = graph.getDependencyResolver().resolveDependency(Config.class);

        config.getDeclaredSettings().values().forEach(setting -> {
            if (!((SettingImpl<?>) setting).internal()
                    && setting.name().toLowerCase().contains(lowerCasedSearchString)) {
                results.add(new ConfigResult(setting, config));
            }
        });
        return results.stream().sorted(Comparator.comparing(c -> c.name));
    }

    @Internal
    @SystemProcedure
    @Description("Return config settings interesting to clients (e.g. Neo4j Browser)")
    @Procedure(name = "dbms.clientConfig", mode = DBMS)
    public Stream<ConfigResult> listClientConfig() {
        List<ConfigResult> results = new ArrayList<>();
        Set<String> browserSettings = Stream.of(
                        "browser.allow_outgoing_connections",
                        "browser.credential_timeout",
                        "browser.retain_connection_credentials",
                        "browser.retain_editor_history",
                        "dbms.security.auth_enabled",
                        "browser.remote_content_hostname_whitelist",
                        "browser.post_connect_cmd",
                        "client.allow_telemetry",
                        "server.metrics.prefix")
                .collect(Collectors.toCollection(HashSet::new));

        Config config = graph.getDependencyResolver().resolveDependency(Config.class);
        config.getDeclaredSettings().values().forEach(setting -> {
            if (browserSettings.contains(setting.name().toLowerCase())) {
                results.add(new ConfigResult(setting, config));
            }
        });
        return results.stream().sorted(Comparator.comparing(c -> c.name));
    }

    @Description("Attaches a map of data to the transaction. The data will be printed when listing queries, and "
            + "inserted into the query log.")
    @Procedure(name = "tx.setMetaData", mode = DBMS)
    public void setTXMetaData(@Name(value = "data") Map<String, Object> data) {
        int totalCharSize = data.entrySet().stream()
                .mapToInt(e -> e.getKey().length()
                        + ((e.getValue() != null) ? e.getValue().toString().length() : 0))
                .sum();

        if (totalCharSize >= HARD_CHAR_LIMIT) {
            throw new IllegalArgumentException(format(
                    "Invalid transaction meta-data, expected the total number of chars for "
                            + "keys and values to be less than %d, got %d",
                    HARD_CHAR_LIMIT, totalCharSize));
        }

        InternalTransaction internalTransaction = (InternalTransaction) this.transaction;

        Config config = graph.getDependencyResolver().resolveDependency(Config.class);
        graph.getDependencyResolver()
                .resolveDependency(TransactionLookup.class)
                .findTransactionContaining(internalTransaction)
                .ifPresentOrElse(parent -> parent.setMetaData(data), () -> internalTransaction.setMetaData(data));
    }

    @SystemProcedure
    @Description("Provides attached transaction metadata.")
    @Procedure(name = "tx.getMetaData", mode = DBMS)
    public Stream<MetadataResult> getTXMetaData() {
        return Stream.of(((InternalTransaction) transaction).kernelTransaction().getMetaData())
                .map(MetadataResult::new);
    }

    @Admin
    @SystemProcedure
    @Description("Clears all query caches.")
    @Procedure(name = "db.clearQueryCaches", mode = DBMS)
    public Stream<StringResult> clearAllQueryCaches() {
        QueryExecutionEngine queryExecutionEngine =
                graph.getDependencyResolver().resolveDependency(QueryExecutionEngine.class);

        // Composite queries and Query Router queries are exclusive.
        // There cannot be a query that is at the same in Composite caches and Query router caches.
        // The reason is that Composite queries don't go through Query Router at all.
        long clearedRouterAndCompositeQueries = 0;
        if (graph.getDependencyResolver().containsDependency(FabricExecutor.class)) {
            FabricExecutor fabricExecutor = graph.getDependencyResolver().resolveDependency(FabricExecutor.class);
            clearedRouterAndCompositeQueries = fabricExecutor.clearQueryCachesForDatabase(graph.databaseName());
        }
        if (graph.getDependencyResolver().containsDependency(QueryRouter.class)) {
            QueryRouter queryRouter = graph.getDependencyResolver().resolveDependency(QueryRouter.class);
            clearedRouterAndCompositeQueries += queryRouter.clearQueryCachesForDatabase(graph.databaseName());
        }

        if (kernelTransaction.isSPDTransaction()) {
            kernelTransaction.clearSPDQueryCaches();
        }

        // we subtract 1 because the query "CALL db.queryClearCaches()" is compiled and thus populates the caches by 1
        long numberOfClearedQueries =
                Math.max(queryExecutionEngine.clearQueryCaches(), clearedRouterAndCompositeQueries) - 1;

        String result = numberOfClearedQueries == 0
                ? "Query cache already empty."
                : "Query caches successfully cleared of " + numberOfClearedQueries + " queries.";
        log.info("Called db.clearQueryCaches(): " + result);
        return Stream.of(new StringResult(result));
    }

    @Deprecated(since = "5.9.0")
    @Admin
    @SystemProcedure
    @Description("Report the current status of the system database sub-graph schema.")
    @Procedure(name = "dbms.upgradeStatus", mode = READ)
    public Stream<SystemGraphComponentStatusResult> upgradeStatus() throws ProcedureException {
        if (!callContext.isSystemDatabase()) {
            throw new ProcedureException(
                    ProcedureCallFailed,
                    "This is an administration command and it should be executed against the system database: dbms.upgradeStatus");
        }
        return Stream.of(getAggregateUpgradeStatus(systemGraphComponents, resolver, transaction));
    }

    @Deprecated(since = "5.9.0")
    @Admin
    @SystemProcedure
    @Description("Upgrade the system database schema if it is not the current schema.")
    @Procedure(name = "dbms.upgrade", mode = WRITE)
    public Stream<SystemGraphComponentUpgradeResult> upgrade() throws ProcedureException {
        if (!callContext.isSystemDatabase()) {
            throw new ProcedureException(
                    ProcedureCallFailed,
                    "This is an administration command and it should be executed against the system database: dbms.upgrade");
        }
        var upgradeCheckResult =
                resolver.resolveDependency(UpgradeChecker.class).upgradeCheck();
        if (!upgradeCheckResult.upgradeAllowed()) {
            log.info("Upgrade not currently possible: %s", upgradeCheckResult.whyUpgradeNotAllowed());
            return Stream.of(new SystemGraphComponentUpgradeResult(
                    SystemGraphComponentStatusResult.CANNOT_UPGRADE_STATUS, upgradeCheckResult.whyUpgradeNotAllowed()));
        }

        SystemGraphComponents components = systemGraphComponents;
        SystemGraphComponent.Status status = components.detect(graph);

        if (isUpgradeable(status)) {

            Config config = graph.getDependencyResolver().resolveDependency(Config.class);
            // Auto-upgrade is an enterprise only feature, and currently has a feature flag.
            if (graph.dbmsInfo().edition != Edition.COMMUNITY && config.get(automatic_upgrade_enabled)) {
                var timeout = config.get(upgrade_procedure_wait_timeout);
                var clock = resolver.resolveDependency(Clock.class);
                // Wait / retry a little to see if automatic upgrade does the upgrade for us
                var laterStatus = waitForUpgrade(() -> components.detect(graph), timeout, clock, log);

                if (isUpgradeable(laterStatus)) {
                    return Stream.of(new SystemGraphComponentUpgradeResult(laterStatus.name(), UPGRADE_PENDING_RESULT));
                } else {
                    return Stream.of(
                            new SystemGraphComponentUpgradeResult(laterStatus.name(), laterStatus.resolution()));
                }
            } else {
                List<String> failed = new ArrayList<>();
                components.forEach(component -> {
                    SystemGraphComponent.Status initialStatus = component.detect(graph);
                    if (isUpgradeable(initialStatus)) {
                        try {
                            component.upgradeToCurrent(graph);
                        } catch (Exception e) {
                            failed.add(String.format("[%s] %s", component.componentName(), e.getMessage()));
                        }
                    }
                });
                String upgradeResult = failed.isEmpty() ? "Success" : "Failed: " + String.join(", ", failed);
                return Stream.of(new SystemGraphComponentUpgradeResult(
                        components.detect(transaction).name(), upgradeResult));
            }
        } else {
            return Stream.of(new SystemGraphComponentUpgradeResult(status.name(), status.resolution()));
        }
    }

    @SystemProcedure
    @Description("List all accepted network connections at this instance that are visible to the user.")
    @Procedure(name = "dbms.listConnections", mode = DBMS)
    public Stream<ListConnectionResult> listConnections() {
        NetworkConnectionTracker connectionTracker = getConnectionTracker();
        ZoneId timeZone = getConfiguredTimeZone();

        return connectionTracker.activeConnections().stream()
                .filter(connection -> isAdminOrSelf(connection.username()))
                .map(connection -> new ListConnectionResult(connection, timeZone));
    }

    @SystemProcedure
    @Description("Kill network connection with the given connection id.")
    @Procedure(name = "dbms.killConnection", mode = DBMS)
    public Stream<ConnectionTerminationResult> killConnection(@Name("id") String id) {
        return killConnections(singletonList(id));
    }

    @SystemProcedure
    @Description("Kill all network connections with the given connection ids.")
    @Procedure(name = "dbms.killConnections", mode = DBMS)
    public Stream<ConnectionTerminationResult> killConnections(@Name("ids") List<String> ids) {
        NetworkConnectionTracker connectionTracker = getConnectionTracker();

        return ids.stream().map(id -> killConnection(id, connectionTracker));
    }

    @Admin
    @Internal
    @SystemProcedure
    @Description("List all capabilities including internals")
    @Procedure(name = "dbms.listAllCapabilities", mode = DBMS)
    public Stream<CapabilityResult> listAllCapabilities() {
        var service = resolver.resolveDependency(CapabilitiesService.class);
        var capabilities = service.declaredCapabilities();

        return capabilities.stream().map(c -> new CapabilityResult(c, service.get(c.name())));
    }

    @SystemProcedure
    @Description("List capabilities.")
    @Procedure(name = "dbms.listCapabilities", mode = DBMS)
    public Stream<CapabilityResult> listCapabilities() {
        var service = resolver.resolveDependency(CapabilitiesService.class);
        var capabilities = service.declaredCapabilities();

        return capabilities.stream()
                .filter(c -> !c.internal())
                .map(c -> new CapabilityResult(c, service.get(c.name())));
    }

    private NetworkConnectionTracker getConnectionTracker() {
        return resolver.resolveDependency(NetworkConnectionTracker.class);
    }

    private ConnectionTerminationResult killConnection(String id, NetworkConnectionTracker connectionTracker) {
        TrackedNetworkConnection connection = connectionTracker.get(id);
        if (connection != null) {
            if (isAdminOrSelf(connection.username())) {
                connection.close();
                return new ConnectionTerminationResult(id, connection.username());
            }

            throw kernelTransaction
                    .securityAuthorizationHandler()
                    .logAndGetAuthorizationException(
                            securityContext,
                            format("Not allowed to terminate connection for user %s.", connection.username()));
        }
        return new ConnectionTerminationFailedResult(id);
    }

    private boolean isAdminOrSelf(String username) {
        return securityContext.allowExecuteAdminProcedure(callContext.id()).allowsAccess()
                || securityContext.subject().hasUsername(username);
    }

    private GraphDatabaseAPI getSystemDatabase() {
        return (GraphDatabaseAPI) graph.getDependencyResolver()
                .resolveDependency(DatabaseManagementService.class)
                .database(SYSTEM_DATABASE_NAME);
    }

    private static StoreIdProvider getSystemDatabaseStoreIdProvider(GraphDatabaseAPI databaseAPI) {
        return databaseAPI.getDependencyResolver().resolveDependency(StoreIdProvider.class);
    }

    private DatabaseContextProvider<DatabaseContext> getDatabaseManager() {
        return (DatabaseContextProvider<DatabaseContext>) resolver.resolveDependency(DatabaseContextProvider.class);
    }

    private ZoneId getConfiguredTimeZone() {
        Config config = graph.getDependencyResolver().resolveDependency(Config.class);
        return config.get(GraphDatabaseSettings.db_timezone).getZoneId();
    }

    /**
     * Get the status result for the overall system graph components. Will check if an upgrade is currently
     * permitted, and report not allowed in addition to the usual statuses.
     */
    public static SystemGraphComponentStatusResult getAggregateUpgradeStatus(
            SystemGraphComponents systemGraphComponents, DependencyResolver resolver, Transaction transaction) {
        var checker = resolver.resolveDependency(UpgradeChecker.class);
        var checkResult = checker.upgradeCheck();
        if (!checkResult.upgradeAllowed()) {
            return new SystemGraphComponentStatusResult(
                    SystemGraphComponentStatusResult.CANNOT_UPGRADE_STATUS,
                    checkResult.whyUpgradeNotAllowed(),
                    SystemGraphComponentStatusResult.CANNOT_UPGRADE_RESOLUTION);
        }
        return new SystemGraphComponentStatusResult(systemGraphComponents.detect(transaction));
    }

    public record SystemInfo(String id, String name, String creationDate) {}

    public static class StringResult {
        public final String value;

        public StringResult(String value) {
            this.value = value;
        }
    }

    public static class MetadataResult {
        public final Map<String, Object> metadata;

        MetadataResult(Map<String, Object> metadata) {
            this.metadata = metadata;
        }
    }

    public record SystemGraphComponentStatusResult(String status, String description, String resolution) {
        public static final String CANNOT_UPGRADE_STATUS = "CANNOT_UPGRADE";
        public static final String CANNOT_UPGRADE_RESOLUTION =
                "Wait for upgraded versions to be observed, or upgrade other cluster members so all are on the same version.";

        SystemGraphComponentStatusResult(SystemGraphComponent.Status status) {
            this(status.name(), status.description(), status.resolution());
        }
    }

    public static class SystemGraphComponentUpgradeResult {
        public final String status;
        public final String upgradeResult;

        SystemGraphComponentUpgradeResult(String status, String upgradeResult) {
            this.status = status;
            this.upgradeResult = upgradeResult;
        }
    }

    public static boolean isUpgradeable(Status status) {
        // New components are not currently initialised in cluster deployment when new binaries are booted on top of an
        // existing database.
        // This is a known shortcoming of the lifecycle and a state transfer from UNINITIALIZED to CURRENT must be
        // supported as a workaround until it is fixed.
        return List.of(REQUIRES_UPGRADE, UNINITIALIZED).contains(status);
    }

    public static Status waitForUpgrade(Supplier<Status> statusSupplier, Duration waitDuration, Clock clock, Log log) {
        var timeout = clock.millis() + waitDuration.toMillis();
        while (clock.millis() < timeout) {
            var status = statusSupplier.get();
            if (!isUpgradeable(status)) {
                return status;
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.info("Wait for upgrade to complete was interrupted", e);
            }
        }
        return statusSupplier.get();
    }
}
