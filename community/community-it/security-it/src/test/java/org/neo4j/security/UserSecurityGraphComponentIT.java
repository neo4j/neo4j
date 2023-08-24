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
package org.neo4j.security;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.automatic_upgrade_enabled;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.dbms.database.ComponentVersion.COMMUNITY_TOPOLOGY_GRAPH_COMPONENT;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.ComponentVersion.MULTI_DATABASE_COMPONENT;
import static org.neo4j.dbms.database.ComponentVersion.SECURITY_USER_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.REQUIRES_UPGRADE;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_40;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_41;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_43D4;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_50;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_ID;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.KeepFirstDuplicateBuilder;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponentWithVersion;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion;
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@TestInstance(PER_CLASS)
class UserSecurityGraphComponentIT {
    @Inject
    @SuppressWarnings("unused")
    private static TestDirectory directory;

    private static DatabaseManagementService dbms;
    private static GraphDatabaseFacade system;
    private static SystemGraphComponents internalSystemGraphComponents;
    private static UserSecurityGraphComponent userSecurityGraphComponent;
    private static AuthManager authManager;
    // Synthetic system graph component representing the whole system
    private static final SystemGraphComponent.Name testComponent = new SystemGraphComponent.Name("test-component");

    @BeforeAll
    static void setup() {
        Config cfg = Config.newBuilder()
                .set(auth_enabled, TRUE)
                .set(automatic_upgrade_enabled, FALSE)
                .build();

        var internalSystemGraphComponentsBuilder = new KeepFirstDuplicateBuilder();

        // register a dummy DBMS runtime component as it is not a subject of this test
        internalSystemGraphComponentsBuilder.register(new StubComponent(DBMS_RUNTIME_COMPONENT.name()));
        // register a dummy user security component as it is manually initialised in every test.
        internalSystemGraphComponentsBuilder.register(new StubComponent(SECURITY_USER_COMPONENT.name()));

        dbms = new TestDatabaseManagementServiceBuilder(directory.homePath())
                .impermanent()
                .setConfig(cfg)
                .setExternalDependencies(dependenciesOf(internalSystemGraphComponentsBuilder))
                .noOpSystemGraphInitializer()
                .build();
        system = (GraphDatabaseFacade) dbms.database(SYSTEM_DATABASE_NAME);
        DependencyResolver resolver = system.getDependencyResolver();
        authManager = resolver.resolveDependency(AuthManager.class);
        userSecurityGraphComponent = new UserSecurityGraphComponent(
                new InMemoryUserRepository(),
                Config.defaults(),
                NullLogProvider.getInstance(),
                CommunitySecurityLog.NULL_LOG);

        internalSystemGraphComponents = resolver.resolveDependency(SystemGraphComponents.class);
    }

    @BeforeEach
    void clear() throws Exception {
        // Clear and re-initialise the system database before every test
        inTx(tx -> Iterables.forEach(tx.getAllNodes(), n -> {
            Iterables.forEach(n.getRelationships(), Relationship::delete);
            n.delete();
        }));
        inTx(tx -> tx.schema().getConstraints().forEach(ConstraintDefinition::drop));

        internalSystemGraphComponents.initializeSystemGraph(system);
    }

    @AfterAll
    static void tearDown() {
        dbms.shutdown();
    }

    @ParameterizedTest
    @MethodSource("supportedPreviousVersions")
    void shouldAuthenticate(UserSecurityGraphComponentVersion version) throws Exception {
        initUserSecurityComponent(version);
        LoginContext loginContext =
                authManager.login(AuthToken.newBasicAuthToken("neo4j", "neo4j"), EMBEDDED_CONNECTION);
        assertThat(loginContext.subject().getAuthenticationResult())
                .isEqualTo(AuthenticationResult.PASSWORD_CHANGE_REQUIRED);
    }

    @Test
    void shouldInitializeDefaultVersion() throws Exception {
        userSecurityGraphComponent.initializeSystemGraph(system, true);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);

        HashMap<SystemGraphComponent.Name, SystemGraphComponent.Status> componentStatuses = new HashMap<>();
        SystemGraphComponent.Name overallStatus =
                new SystemGraphComponent.Name("overall-status"); // aggregated status, rather than a real component
        inTx(tx -> {
            systemGraphComponents.forEach(
                    component -> componentStatuses.put(component.componentName(), component.detect(tx)));
            componentStatuses.put(overallStatus, systemGraphComponents.detect(tx));
        });

        var expectedComponents = Set.of(
                DBMS_RUNTIME_COMPONENT,
                MULTI_DATABASE_COMPONENT,
                SECURITY_USER_COMPONENT,
                COMMUNITY_TOPOLOGY_GRAPH_COMPONENT,
                overallStatus);
        assertThat(componentStatuses.keySet()).containsExactlyInAnyOrderElementsOf(expectedComponents);
        for (SystemGraphComponent.Name component : expectedComponents) {
            assertThat(componentStatuses.get(component))
                    .as("Component status should all be current")
                    .isEqualTo(CURRENT);
        }
    }

    @ParameterizedTest
    @MethodSource("versionAndStatusProvider")
    void shouldInitializeAndUpgradeSystemGraph(
            UserSecurityGraphComponentVersion version, SystemGraphComponent.Status initialStatus) throws Exception {
        initUserSecurityComponent(version);
        assertCanUpgradeThisVersionAndThenUpgradeIt(initialStatus);
    }

    static Stream<Arguments> versionAndStatusProvider() {
        return Stream.of(
                Arguments.arguments(COMMUNITY_SECURITY_40, REQUIRES_UPGRADE),
                Arguments.arguments(COMMUNITY_SECURITY_41, REQUIRES_UPGRADE),
                Arguments.arguments(COMMUNITY_SECURITY_43D4, REQUIRES_UPGRADE),
                Arguments.arguments(COMMUNITY_SECURITY_50, CURRENT));
    }

    @ParameterizedTest
    @MethodSource("beforeUserId")
    void shouldAddUserIdsOnUpgradeFromOlderSystemDb(UserSecurityGraphComponentVersion version) throws Exception {
        // Given
        initUserSecurityComponent(version);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);

        createUser(version, "alice");

        // Then
        HashMap<String, Object> usernameAndIdsBeforeUpgrade = getUserNamesAndIds();
        assertThat(usernameAndIdsBeforeUpgrade.get("neo4j")).isNull();
        assertThat(usernameAndIdsBeforeUpgrade.get("alice")).isNull();

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then
        HashMap<String, Object> usernameAndIdsAfterUpgrade = getUserNamesAndIds();

        assertThat(usernameAndIdsAfterUpgrade.get("neo4j")).isNotNull();
        assertThat(usernameAndIdsAfterUpgrade.get("alice")).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("beforeUserIdConstraint")
    void shouldAddConstraintForUserIdsOnUpgradeFromOlderSystemDb(UserSecurityGraphComponentVersion version)
            throws Exception {
        // Given
        initUserSecurityComponent(version);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);

        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints(USER_LABEL);
            for (ConstraintDefinition constraint : constraints) {
                for (String property : constraint.getPropertyKeys()) {
                    assertThat(property).isIn("name");
                }
            }
            tx.commit();
        }

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints(USER_LABEL);
            for (ConstraintDefinition constraint : constraints) {
                for (String property : constraint.getPropertyKeys()) {
                    assertThat(property).isIn("name", USER_ID);
                }
            }
            tx.commit();
        }
    }

    private static SystemGraphComponents systemGraphComponentsPlus(
            SystemGraphComponents existing, SystemGraphComponent... components) {
        var builder = new KeepFirstDuplicateBuilder();
        for (var component : components) {
            builder.register(component);
        }
        existing.forEach(builder::register);
        return builder.build();
    }

    private static Stream<Arguments> supportedPreviousVersions() {
        return Arrays.stream(UserSecurityGraphComponentVersion.values())
                .filter(version -> version.runtimeSupported() && !version.isCurrent(Config.defaults()))
                .map(Arguments::of);
    }

    private static Stream<Arguments> beforeUserId() {
        return Arrays.stream(UserSecurityGraphComponentVersion.values())
                .filter(version ->
                        version.runtimeSupported() && version.getVersion() < COMMUNITY_SECURITY_43D4.getVersion())
                .map(Arguments::of);
    }

    private static Stream<Arguments> beforeUserIdConstraint() {
        return Arrays.stream(UserSecurityGraphComponentVersion.values())
                .filter(version ->
                        version.runtimeSupported() && version.getVersion() < COMMUNITY_SECURITY_50.getVersion())
                .map(Arguments::of);
    }

    private static void assertCanUpgradeThisVersionAndThenUpgradeIt(SystemGraphComponent.Status initialState)
            throws Exception {
        var internalSystemGraphComponents =
                system.getDependencyResolver().resolveDependency(SystemGraphComponents.class);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);
        assertStatus(
                systemGraphComponents,
                Map.of(
                        DBMS_RUNTIME_COMPONENT,
                        CURRENT,
                        MULTI_DATABASE_COMPONENT,
                        CURRENT,
                        SECURITY_USER_COMPONENT,
                        initialState,
                        COMMUNITY_TOPOLOGY_GRAPH_COMPONENT,
                        CURRENT,
                        testComponent,
                        initialState));

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then when looking at component statuses
        assertStatus(
                systemGraphComponents,
                Map.of(
                        DBMS_RUNTIME_COMPONENT,
                        CURRENT,
                        MULTI_DATABASE_COMPONENT,
                        CURRENT,
                        SECURITY_USER_COMPONENT,
                        CURRENT,
                        COMMUNITY_TOPOLOGY_GRAPH_COMPONENT,
                        CURRENT,
                        testComponent,
                        CURRENT));
    }

    private static void assertStatus(
            SystemGraphComponents systemGraphComponents,
            Map<SystemGraphComponent.Name, SystemGraphComponent.Status> expected)
            throws Exception {
        HashMap<SystemGraphComponent.Name, SystemGraphComponent.Status> statuses = new HashMap<>();
        inTx(tx -> {
            systemGraphComponents.forEach(component -> statuses.put(component.componentName(), component.detect(tx)));
            statuses.put(testComponent, systemGraphComponents.detect(tx));
        });
        assertThat(statuses).isEqualTo(expected);
    }

    private HashMap<String, Object> getUserNamesAndIds() {
        HashMap<String, Object> usernameAndIds = new HashMap<>();

        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED);
                ResourceIterator<Node> nodes = tx.findNodes(USER_LABEL)) {
            while (nodes.hasNext()) {
                Node userNode = nodes.next();
                String username = userNode.getProperty("name").toString();
                Object userId;
                try {
                    userId = userNode.getProperty("id");
                } catch (NotFoundException e) {
                    userId = null;
                }
                usernameAndIds.put(username, userId);
            }
        }
        return usernameAndIds;
    }

    private void createUser(UserSecurityGraphComponentVersion version, String name) {
        KnownCommunitySecurityComponentVersion builder =
                userSecurityGraphComponent.findSecurityGraphComponentVersion(version);
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            builder.addUser(tx, name, credentialFor("abc123"), false, false);
            tx.commit();
        }
    }

    private static void initUserSecurityComponent(UserSecurityGraphComponentVersion version) throws Exception {
        KnownCommunitySecurityComponentVersion builder =
                userSecurityGraphComponent.findSecurityGraphComponentVersion(version);
        inTx(tx -> tx.schema()
                .constraintFor(USER_LABEL)
                .assertPropertyIsUnique("name")
                .create());

        inTx(builder::setupUsers);
        if (version != COMMUNITY_SECURITY_40) {
            inTx(tx -> builder.setVersionProperty(tx, version.getVersion()));
        }
        if (version.getVersion() >= COMMUNITY_SECURITY_50.getVersion()) {
            inTx(tx -> tx.schema()
                    .constraintFor(USER_LABEL)
                    .assertPropertyIsUnique(USER_ID)
                    .create());
        }
        userSecurityGraphComponent.postInitialization(system, true);
    }

    private static void inTx(ThrowingConsumer<Transaction, Exception> consumer) throws Exception {
        try (Transaction tx = system.beginTx()) {
            consumer.accept(tx);
            tx.commit();
        }
    }

    static class StubComponent implements SystemGraphComponentWithVersion {

        private final SystemGraphComponent.Name name;

        public StubComponent(String name) {
            this.name = new SystemGraphComponent.Name(name);
        }

        @Override
        public Name componentName() {
            return name;
        }

        @Override
        public int getLatestSupportedVersion() {
            return 0;
        }

        @Override
        public Status detect(Transaction tx) {
            return CURRENT;
        }

        @Override
        public void initializeSystemGraph(GraphDatabaseService system, boolean firstInitialization) {
            // NO-OP
        }

        @Override
        public void upgradeToCurrent(GraphDatabaseService system) {
            // NO-OP
        }
    }
}
