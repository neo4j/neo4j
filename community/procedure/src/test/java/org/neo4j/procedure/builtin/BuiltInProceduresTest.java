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

import static java.util.Collections.emptyIterator;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.upgrade_procedure_wait_timeout;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTNode;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTPath;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTRelationship;
import static org.neo4j.kernel.api.ResourceTracker.EMPTY_RESOURCE_TRACKER;
import static org.neo4j.kernel.api.procedure.BasicContext.buildContext;
import static org.neo4j.procedure.builtin.BuiltInDbmsProcedures.SystemGraphComponentStatusResult.CANNOT_UPGRADE_RESOLUTION;
import static org.neo4j.procedure.builtin.BuiltInDbmsProcedures.SystemGraphComponentStatusResult.CANNOT_UPGRADE_STATUS;
import static org.neo4j.procedure.builtin.BuiltInDbmsProcedures.UPGRADE_PENDING_RESULT;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.TestSystemGraphComponent;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.CypherScope;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Log;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures.UpgradeAllowedChecker;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures.UpgradeAllowedChecker.UpgradeAlwaysAllowed;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures.UpgradeAllowedChecker.UpgradeNotAllowedException;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;
import org.neo4j.procedure.impl.ProcedureConfig;
import org.neo4j.procedure.impl.temporal.TemporalFunction;
import org.neo4j.time.Clocks;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;

class BuiltInProceduresTest {
    private final Map<Integer, String> labels = new HashMap<>();
    private final Map<Integer, String> propKeys = new HashMap<>();
    private final Map<Integer, String> relTypes = new HashMap<>();
    private final Read read = mock(Read.class);
    private final TokenRead tokens = mock(TokenRead.class);
    private final SchemaRead schemaRead = mock(SchemaRead.class);
    private final SchemaReadCore schemaReadCore = mock(SchemaReadCore.class);
    private final InternalTransaction transaction = mock(InternalTransaction.class);
    private final KernelTransaction tx = mock(KernelTransaction.class);
    private final ProcedureCallContext callContext = mock(ProcedureCallContext.class);
    private final DependencyResolver resolver = mock(DependencyResolver.class);
    private final GraphDatabaseAPI graphDatabaseAPI = mock(GraphDatabaseAPI.class);
    private final IndexingService indexingService = mock(IndexingService.class);
    private final Clock clock = Clocks.tickOnAccessClock(Instant.now(), Duration.ofSeconds(1));
    private final Log log = mock(InternalLog.class);
    private final GlobalProceduresRegistry procs = new GlobalProceduresRegistry();
    private SystemGraphComponents systemGraphComponents = new SystemGraphComponents.DefaultBuilder().build();

    @BeforeEach
    void setup() throws Exception {
        procs.registerComponent(KernelTransaction.class, Context::kernelTransaction, false);
        procs.registerComponent(DependencyResolver.class, Context::dependencyResolver, false);
        procs.registerComponent(GraphDatabaseAPI.class, Context::graphDatabaseAPI, false);
        procs.registerComponent(
                Transaction.class, ctx -> ctx.kernelTransaction().internalTransaction(), true);
        procs.registerComponent(SecurityContext.class, Context::securityContext, true);
        procs.registerComponent(ProcedureCallContext.class, Context::procedureCallContext, true);
        procs.registerComponent(SystemGraphComponents.class, ctx -> systemGraphComponents, false);

        procs.registerComponent(Log.class, ctx -> log, false);
        procs.registerType(Node.class, NTNode);
        procs.registerType(Relationship.class, NTRelationship);
        procs.registerType(Path.class, NTPath);

        doReturn(clock).when(resolver).resolveDependency(Clock.class);

        var builtins = SpecialBuiltInProcedures.from("1.3.37", Edition.COMMUNITY.toString());
        for (var proc : builtins.get()) {
            procs.register(proc);
        }
        procs.registerProcedure(BuiltInProcedures.class);
        procs.registerProcedure(BuiltInDbmsProcedures.class);

        when(transaction.kernelTransaction()).thenReturn(tx);
        when(tx.tokenRead()).thenReturn(tokens);
        when(tx.dataRead()).thenReturn(read);
        when(tx.schemaRead()).thenReturn(schemaRead);
        when(tx.securityContext()).thenReturn(SecurityContext.AUTH_DISABLED);
        when(tx.internalTransaction()).thenReturn(transaction);
        when(callContext.isCalledFromCypher()).thenReturn(false);
        when(schemaRead.snapshot()).thenReturn(schemaReadCore);

        when(tokens.propertyKeyGetAllTokens()).thenAnswer(asTokens(propKeys));
        when(tokens.labelsGetAllTokens()).thenAnswer(asTokens(labels));
        when(tokens.relationshipTypesGetAllTokens()).thenAnswer(asTokens(relTypes));

        when(tokens.propertyKeyName(anyInt())).thenAnswer(invocation -> propKeys.get(invocation.getArgument(0)));
        when(tokens.nodeLabelName(anyInt())).thenAnswer(invocation -> labels.get(invocation.getArgument(0)));
        when(tokens.relationshipTypeName(anyInt())).thenAnswer(invocation -> relTypes.get(invocation.getArgument(0)));
        when(tokens.propertyKeyGetName(anyInt())).thenAnswer(invocation -> propKeys.get(invocation.getArgument(0)));
        when(tokens.labelGetName(anyInt())).thenAnswer(invocation -> labels.get(invocation.getArgument(0)));
        when(tokens.relationshipTypeGetName(anyInt()))
                .thenAnswer(invocation -> relTypes.get(invocation.getArgument(0)));
        when(tokens.entityTokensGetNames(any(), any())).then(invocation -> {
            EntityType type = invocation.getArgument(0);
            int[] ids = invocation.getArgument(1);
            Map<Integer, String> mapping = type == EntityType.NODE ? labels : relTypes;
            return Arrays.stream(ids).mapToObj(mapping::get).toArray(String[]::new);
        });

        when(schemaReadCore.constraintsGetForRelationshipType(anyInt())).thenReturn(emptyIterator());
        when(schemaReadCore.indexesGetForLabel(anyInt())).thenReturn(emptyIterator());
        when(schemaReadCore.indexesGetForRelationshipType(anyInt())).thenReturn(emptyIterator());
        when(schemaReadCore.constraintsGetForLabel(anyInt())).thenReturn(emptyIterator());
        when(read.countsForNode(anyInt())).thenReturn(1L);
        when(read.countsForRelationship(anyInt(), anyInt(), anyInt())).thenReturn(1L);
        when(schemaReadCore.indexGetState(any(IndexDescriptor.class))).thenReturn(InternalIndexState.ONLINE);

        when(graphDatabaseAPI.dbmsInfo()).thenReturn(DbmsInfo.ENTERPRISE);
    }

    @Test
    void lookupComponentProviders() {
        var view = procs.getCurrentView();
        assertNotNull(view.lookupComponentProvider(Transaction.class, true));
        assertNotNull(view.lookupComponentProvider(Transaction.class, false));

        assertNull(view.lookupComponentProvider(Statement.class, true));
        assertNull(view.lookupComponentProvider(Statement.class, false));

        assertNull(view.lookupComponentProvider(DependencyResolver.class, true));
        assertNotNull(view.lookupComponentProvider(DependencyResolver.class, false));
    }

    @Test
    void shouldListPropertyKeys() throws Throwable {
        // Given
        givenPropertyKeys("name", "age");

        // When/Then
        assertThat(call("db.propertyKeys")).contains(record("age"), record("name"));
    }

    @Test
    void shouldListLabels() throws Throwable {
        // Given
        givenLabels("Banana", "Fruit");

        // When/Then
        assertThat(call("db.labels")).contains(record("Banana"), record("Fruit"));
    }

    @Test
    void shouldListRelTypes() throws Throwable {
        // Given
        givenRelationshipTypes("EATS", "SPROUTS");

        // When/Then
        assertThat(call("db.relationshipTypes")).contains(record("EATS"), record("SPROUTS"));
    }

    @Test
    void shouldListSystemComponents() throws Throwable {
        // When/Then
        assertThat(call("dbms.components")).contains(record("Neo4j Kernel", singletonList("1.3.37"), "community"));
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbLabels() {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when(tokens.labelsGetAllTokens()).thenThrow(runtimeException);

        // When
        assertThrows(ProcedureException.class, () -> call("db.labels"));
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbPropertyKeys() {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when(tokens.propertyKeyGetAllTokens()).thenThrow(runtimeException);

        // When
        assertThrows(ProcedureException.class, () -> call("db.propertyKeys"));
    }

    @Test
    void shouldCloseStatementIfExceptionIsThrownDbRelationshipTypes() {
        // Given
        RuntimeException runtimeException = new RuntimeException();
        when(tokens.relationshipTypesGetAllTokens()).thenThrow(runtimeException);

        // When
        assertThrows(ProcedureException.class, () -> call("db.relationshipTypes"));
    }

    @Test
    void shouldPing() throws ProcedureException, IndexNotFoundKernelException {
        assertThat(call("db.ping")).containsExactly(record(Boolean.TRUE));
    }

    @Test
    void listClientConfigShouldFilterConfig() throws ProcedureException, IndexNotFoundKernelException {
        // Given
        Config mockConfig = mock(Config.class);
        HashMap<Setting<?>, Object> settings = new HashMap<>();

        settings.put(
                SettingImpl.newBuilder("browser.allow_outgoing_connections", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("browser.credential_timeout", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("browser.retain_connection_credentials", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("browser.retain_editor_history", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("dbms.security.auth_enabled", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("browser.remote_content_hostname_whitelist", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("browser.post_connect_cmd", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("initial.dbms.default_database", SettingValueParsers.STRING, "")
                        .build(),
                "");
        settings.put(
                SettingImpl.newBuilder("something.else", SettingValueParsers.STRING, "")
                        .build(),
                "");

        HashMap<String, Setting<Object>> declaredSettings = new HashMap<>();
        settings.forEach((setting, value) -> declaredSettings.put(setting.name(), (Setting<Object>) setting));

        when(mockConfig.getDeclaredSettings()).thenReturn(declaredSettings);
        when(resolver.resolveDependency(Config.class)).thenReturn(mockConfig);

        // When / Then
        assertThat(call("dbms.clientConfig"))
                .containsExactlyInAnyOrder(
                        record(
                                "browser.allow_outgoing_connections",
                                "browser.allow_outgoing_connections, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "browser.credential_timeout",
                                "browser.credential_timeout, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "browser.retain_connection_credentials",
                                "browser.retain_connection_credentials, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "browser.retain_editor_history",
                                "browser.retain_editor_history, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "dbms.security.auth_enabled",
                                "dbms.security.auth_enabled, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "browser.remote_content_hostname_whitelist",
                                "browser.remote_content_hostname_whitelist, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."),
                        record(
                                "browser.post_connect_cmd",
                                "browser.post_connect_cmd, a string.",
                                null,
                                false,
                                null,
                                null,
                                false,
                                "A string."));
    }

    @Test
    void shouldNotListSystemGraphComponentsIfNotSystemDb() {
        Config config = Config.defaults();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(callContext.isSystemDatabase()).thenReturn(false);

        assertThatThrownBy(() -> call("dbms.upgradeStatus"))
                .isInstanceOf(ProcedureException.class)
                .hasMessage(
                        "This is an administration command and it should be executed against the system database: dbms.upgradeStatus");
    }

    @Test
    void shouldListSystemGraphComponents() throws ProcedureException, IndexNotFoundKernelException {
        Config config = Config.defaults();
        setupFakeSystemComponents();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeAlwaysAllowed());
        when(callContext.isSystemDatabase()).thenReturn(true);

        var r = call("dbms.upgradeStatus").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String description = resultAsString(row, 1);
        String resolution = resultAsString(row, 2);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains(Status.REQUIRES_UPGRADE.name());
        assertThat(description).contains(Status.REQUIRES_UPGRADE.description());
        assertThat(resolution).contains(Status.REQUIRES_UPGRADE.resolution());
    }

    @Test
    void givenUpgradeNotAllowed_whenCallUpgradeStatus_thenGetNotAllowed()
            throws ProcedureException, IndexNotFoundKernelException {
        Config config = Config.defaults();
        setupFakeSystemComponents();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        var message = "You will never succeed!";
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeNeverAllowed(message));
        when(callContext.isSystemDatabase()).thenReturn(true);

        var r = call("dbms.upgradeStatus").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String description = resultAsString(row, 1);
        String resolution = resultAsString(row, 2);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).isEqualTo(CANNOT_UPGRADE_STATUS);
        assertThat(description).contains(message);
        assertThat(resolution).isEqualTo(CANNOT_UPGRADE_RESOLUTION);
    }

    @Test
    void shouldNotUpgradeSystemGraphIfNotSystemDb() {
        // Given
        Config config = Config.defaults();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);

        when(callContext.isSystemDatabase()).thenReturn(false);

        assertThatThrownBy(() -> call("dbms.upgrade"))
                .isInstanceOf(ProcedureException.class)
                .hasMessage(
                        "This is an administration command and it should be executed against the system database: dbms.upgrade");
    }

    @Test
    void givenAutoUpgradeDisabledAndUpgradeAllowed_whenUpgrade_shouldUpgrade()
            throws ProcedureException, IndexNotFoundKernelException {
        Config config = Config.defaults();
        config.set(automatic_upgrade_enabled, false);
        setupFakeSystemComponents();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeAlwaysAllowed());
        when(callContext.isSystemDatabase()).thenReturn(true);
        when(graphDatabaseAPI.beginTx()).thenReturn(transaction);

        var r = call("dbms.upgrade").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String result = resultAsString(row, 1);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains(Status.REQUIRES_UPGRADE.name());
        assertThat(result).contains("Failed: [component_D] Upgrade failed because this is a test");
    }

    @Test
    void givenAutoUpgradeEnabledAndUpgradeAllowed_whenUpgrade_shouldWaitForUpgrade()
            throws ProcedureException, IndexNotFoundKernelException {
        Config config = Config.defaults();
        config.set(automatic_upgrade_enabled, true);
        mockSystemGraphComponents(Status.REQUIRES_UPGRADE, Status.REQUIRES_UPGRADE, Status.CURRENT);
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeAlwaysAllowed());
        when(callContext.isSystemDatabase()).thenReturn(true);
        when(graphDatabaseAPI.beginTx()).thenReturn(transaction);

        var r = call("dbms.upgrade").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String result = resultAsString(row, 1);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains(Status.CURRENT.name());
        assertThat(result).isEqualTo(Status.CURRENT.resolution());
    }

    @Test
    void givenAutoUpgradeEnabledAndUpgradeAllowed_whenUpgrade_shouldWaitForUpgradeButWaitTimesOut()
            throws ProcedureException, IndexNotFoundKernelException {
        Config config = Config.defaults();
        config.set(automatic_upgrade_enabled, true);
        config.set(upgrade_procedure_wait_timeout, Duration.ofSeconds(2));
        mockSystemGraphComponents(Status.REQUIRES_UPGRADE);
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeAlwaysAllowed());
        when(callContext.isSystemDatabase()).thenReturn(true);
        when(graphDatabaseAPI.beginTx()).thenReturn(transaction);

        var r = call("dbms.upgrade").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String result = resultAsString(row, 1);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains(Status.REQUIRES_UPGRADE.name());
        assertThat(result).contains(UPGRADE_PENDING_RESULT);
    }

    @Test
    void givenAutoUpgradeDisabledAndUpgradeNotAllowed_shouldNotUpgradeSystemGraph()
            throws ProcedureException, IndexNotFoundKernelException {
        var failureMessage = "Don't want to";
        Config config = Config.defaults();
        when(resolver.resolveDependency(Config.class)).thenReturn(config);
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(() -> {
            throw new UpgradeNotAllowedException(failureMessage);
        });
        when(callContext.isSystemDatabase()).thenReturn(true);
        when(graphDatabaseAPI.beginTx()).thenReturn(transaction);

        var r = call("dbms.upgrade").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String result = resultAsString(row, 1);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains("CANNOT_UPGRADE");
        assertThat(result).contains(failureMessage);
    }

    @Test
    void givenCommunityEdition_whenUpgrade_shouldUpgrade() throws ProcedureException, IndexNotFoundKernelException {
        when(graphDatabaseAPI.dbmsInfo()).thenReturn(DbmsInfo.COMMUNITY);

        setupFakeSystemComponents();
        when(resolver.resolveDependency(UpgradeAllowedChecker.class)).thenReturn(new UpgradeAlwaysAllowed());
        when(callContext.isSystemDatabase()).thenReturn(true);
        when(graphDatabaseAPI.beginTx()).thenReturn(transaction);

        var r = call("dbms.upgrade").iterator();
        assertThat(r.hasNext()).isEqualTo(true).describedAs("Expected one result");
        Object[] row = r.next();
        String status = resultAsString(row, 0);
        String result = resultAsString(row, 1);
        assertThat(r.hasNext()).isEqualTo(false).describedAs("Expected only one result");
        assertThat(status).contains(Status.REQUIRES_UPGRADE.name());
        assertThat(result).contains("Failed: [component_D] Upgrade failed because this is a test");
    }

    @Test
    void temporalFunctionsShouldBeBuiltin() throws Exception {
        GlobalProcedures reg = new GlobalProceduresRegistry();
        TemporalFunction.registerTemporalFunctions(reg, ProcedureConfig.DEFAULT);
        var view = reg.getCurrentView();
        assertThat(view.getAllNonAggregatingFunctions(CypherScope.CYPHER_5).filter(f -> !f.isBuiltIn()))
                .isEmpty();
        assertThat(view.getAllAggregatingFunctions(CypherScope.CYPHER_5).filter(f -> !f.isBuiltIn()))
                .isEmpty();
    }

    private static Object[] record(Object... fields) {
        return fields;
    }

    private void givenPropertyKeys(String... keys) {
        for (String key : keys) {
            token(key, propKeys);
        }
    }

    private void givenLabels(String... labelNames) {
        for (String key : labelNames) {
            token(key, labels);
        }
    }

    private void givenRelationshipTypes(String... types) {
        for (String key : types) {
            token(key, relTypes);
        }
    }

    private static Integer token(String name, Map<Integer, String> tokens) {
        IntSupplier allocateFromMap = () -> {
            int newIndex = tokens.size();
            tokens.put(newIndex, name);
            return newIndex;
        };
        return tokens.entrySet().stream()
                .filter(entry -> entry.getValue().equals(name))
                .mapToInt(Map.Entry::getKey)
                .findFirst()
                .orElseGet(allocateFromMap);
    }

    private static Answer<Iterator<NamedToken>> asTokens(Map<Integer, String> tokens) {
        return i -> tokens.entrySet().stream()
                .map(entry -> new NamedToken(entry.getValue(), entry.getKey()))
                .iterator();
    }

    private List<Object[]> call(String name, Object... args) throws ProcedureException, IndexNotFoundKernelException {
        DefaultValueMapper valueMapper = new DefaultValueMapper(mock(InternalTransaction.class));
        Context ctx = buildContext(resolver, valueMapper)
                .withKernelTransaction(tx)
                .withGraphDatabaseSupplier(() -> graphDatabaseAPI)
                .withProcedureCallContext(callContext)
                .context();

        when(graphDatabaseAPI.getDependencyResolver()).thenReturn(resolver);
        when(resolver.resolveDependency(GraphDatabaseAPI.class)).thenReturn(graphDatabaseAPI);
        when(resolver.resolveDependency(GlobalProcedures.class)).thenReturn(procs);
        when(resolver.resolveDependency(IndexingService.class)).thenReturn(indexingService);
        when(schemaReadCore.indexGetPopulationProgress(any(IndexDescriptor.class)))
                .thenReturn(PopulationProgress.DONE);
        AnyValue[] input = Arrays.stream(args).map(ValueUtils::of).toArray(AnyValue[]::new);
        var view = procs.getCurrentView();
        int procId = view.procedure(ProcedureSignature.procedureName(name.split("\\.")), CypherScope.CYPHER_5)
                .id();
        List<AnyValue[]> anyValues = Iterators.asList(view.callProcedure(ctx, procId, input, EMPTY_RESOURCE_TRACKER));
        List<Object[]> toReturn = new ArrayList<>(anyValues.size());
        for (AnyValue[] anyValue : anyValues) {
            Object[] values = new Object[anyValue.length];
            for (int i = 0; i < anyValue.length; i++) {
                AnyValue value = anyValue[i];
                values[i] = value.map(valueMapper);
            }
            toReturn.add(values);
        }
        return toReturn;
    }

    private static String resultAsString(Object[] row, int index) {
        Object result = row[index];
        if (result instanceof TextValue) {
            return ((TextValue) result).stringValue();
        } else if (result instanceof Value) {
            return ((Value) result).asObjectCopy().toString();
        } else {
            return result.toString();
        }
    }

    private static SystemGraphComponent makeSystemComponentCurrent(String component) {
        var componentName = new SystemGraphComponent.Name(component);
        return new TestSystemGraphComponent(componentName, Status.CURRENT, null, null);
    }

    @SuppressWarnings("SameParameterValue")
    private static SystemGraphComponent makeSystemComponentUpgradeSucceeds(String component) {
        var componentName = new SystemGraphComponent.Name(component);
        return new TestSystemGraphComponent(componentName, Status.REQUIRES_UPGRADE, null, null);
    }

    @SuppressWarnings("SameParameterValue")
    private static SystemGraphComponent makeSystemComponentUpgradeFails(String component) {
        var componentName = new SystemGraphComponent.Name(component);
        return new TestSystemGraphComponent(
                componentName,
                Status.REQUIRES_UPGRADE,
                null,
                new RuntimeException("Upgrade failed because this is a test"));
    }

    private void mockSystemGraphComponents(Status status, Status... statuses) {
        systemGraphComponents = mock(SystemGraphComponents.class);
        doReturn(status, (Object[]) statuses).when(systemGraphComponents).detect(any(GraphDatabaseAPI.class));
    }

    private void setupFakeSystemComponents() {
        var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
        systemGraphComponentsBuilder.register(makeSystemComponentCurrent("component_A"));
        systemGraphComponentsBuilder.register(makeSystemComponentCurrent("component_B"));
        systemGraphComponentsBuilder.register(makeSystemComponentUpgradeSucceeds("component_C"));
        systemGraphComponentsBuilder.register(makeSystemComponentUpgradeFails("component_D"));
        systemGraphComponents = systemGraphComponentsBuilder.build();
    }

    static class UpgradeNeverAllowed implements UpgradeAllowedChecker {

        private final String message;

        public UpgradeNeverAllowed(String message) {
            this.message = message;
        }

        @Override
        public void isUpgradeAllowed() throws UpgradeNotAllowedException {
            throw new UpgradeNotAllowedException(message);
        }
    }
}
