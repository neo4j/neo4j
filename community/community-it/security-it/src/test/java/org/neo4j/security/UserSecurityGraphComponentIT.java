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
import static org.assertj.core.api.Assertions.assertThatIterable;
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
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;
import static org.neo4j.server.security.systemgraph.SecurityGraphHelper.NATIVE_AUTH;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_43D4;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_50;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.COMMUNITY_SECURITY_521;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_CONSTRAINT;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_ID;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_LABEL;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.AUTH_PROVIDER;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.HAS_AUTH;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_ID;
import static org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.KeepFirstDuplicateBuilder;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponentWithVersion;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
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

    @ParameterizedTest
    @MethodSource("versionAndStatusProvider")
    void shouldInitializeAndUpgradeSystemGraphWithoutDefaultUser(
            UserSecurityGraphComponentVersion version, SystemGraphComponent.Status initialStatus) throws Exception {
        var config = Config.defaults();
        config.set(GraphDatabaseInternalSettings.create_default_user, false);
        initUserSecurityComponent(version, config);
        assertCanUpgradeThisVersionAndThenUpgradeIt(initialStatus);
    }

    static Stream<Arguments> versionAndStatusProvider() {
        return Stream.of(
                Arguments.arguments(COMMUNITY_SECURITY_43D4, REQUIRES_UPGRADE),
                Arguments.arguments(COMMUNITY_SECURITY_50, REQUIRES_UPGRADE),
                Arguments.arguments(COMMUNITY_SECURITY_521, CURRENT));
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

    @ParameterizedTest
    @MethodSource("beforeUserAuth")
    void shouldAddUserAuthOnUpgradeFromOlderSystemDb(UserSecurityGraphComponentVersion version) throws Exception {
        // Given
        initUserSecurityComponent(version);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);

        KnownCommunitySecurityComponentVersion builder =
                userSecurityGraphComponent.findSecurityGraphComponentVersion(version);
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            builder.addUser(tx, "alice", credentialFor("abc123"), false, false);
            tx.commit();
        }

        // Then
        assertThat(hasSingleNativeAuthWithUserId("neo4j")).isFalse();
        assertThat(hasSingleNativeAuthWithUserId("alice")).isFalse();

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then
        assertThat(hasSingleNativeAuthWithUserId("neo4j")).isTrue();
        assertThat(hasSingleNativeAuthWithUserId("alice")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("beforeUserAuth")
    void shouldNotGetDuplicateAuthObjectOnUpgradeFromOlderSystemDb(UserSecurityGraphComponentVersion version)
            throws Exception {
        // Given
        initUserSecurityComponent(version);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            tx.execute("ALTER USER neo4j SET PASSWORD 'abcd1234'").close();
            tx.commit();
        }
        assertThat(hasSingleNativeAuthWithUserId("neo4j")).isTrue();

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then
        assertThat(hasSingleNativeAuthWithUserId("neo4j")).isTrue();
    }

    @ParameterizedTest
    @MethodSource("beforeUserAuth")
    void shouldAddConstraintForUserAuthOnUpgradeFromOlderSystemDb(UserSecurityGraphComponentVersion version)
            throws Exception {
        // Given
        initUserSecurityComponent(version);
        var systemGraphComponents =
                systemGraphComponentsPlus(internalSystemGraphComponents, userSecurityGraphComponent);

        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints(AUTH_LABEL);
            assertThatIterable(constraints).isEmpty();
            tx.commit();
        }

        // When running dbms.upgrade
        systemGraphComponents.upgradeToCurrent(system);

        // Then
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            Iterator<ConstraintDefinition> constraints =
                    tx.schema().getConstraints(AUTH_LABEL).iterator();
            assertThat(constraints).hasNext();
            ConstraintDefinition constraint = constraints.next();
            assertThatIterable(constraint.getPropertyKeys()).containsExactlyInAnyOrder(AUTH_ID, AUTH_PROVIDER);
            assertThat(constraints).isExhausted();
            tx.commit();
        }
    }

    @Test
    void shouldInitializeLatestCorrectly() {
        KeepFirstDuplicateBuilder builder = new KeepFirstDuplicateBuilder();
        builder.register(userSecurityGraphComponent);
        SystemGraphComponents systemGraphComponents = builder.build();

        // When
        systemGraphComponents.initializeSystemGraph(system);

        // Then
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            // Has user with id and linked native auth node with the same id
            Node user = tx.findNode(USER_LABEL, "name", "neo4j");
            var userId = user.getProperty(USER_ID, null);
            assertThat(userId).isNotNull();

            Relationship authRel = user.getSingleRelationship(HAS_AUTH, OUTGOING);
            assertThat(authRel).isNotNull();
            Node authNode = authRel.getEndNode();
            var provider = authNode.getProperty(AUTH_PROVIDER);
            var authId = authNode.getProperty(AUTH_ID, 0);

            assertThat(provider).isEqualTo(NATIVE_AUTH);
            assertThat(authId).isEqualTo(userId);

            // CONSTRAINT FOR (user:User) REQUIRE user.id IS UNIQUE
            // CONSTRAINT FOR (user:User) REQUIRE user.name IS UNIQUE
            Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints(USER_LABEL);
            for (ConstraintDefinition constraint : constraints) {
                for (String property : constraint.getPropertyKeys()) {
                    assertThat(property).isIn("name", USER_ID);
                }
            }

            // CONSTRAINT FOR (auth:Auth) REQUIRE (auth.id, auth.provider) IS UNIQUE
            Iterator<ConstraintDefinition> authConstraint =
                    tx.schema().getConstraints(AUTH_LABEL).iterator();
            assertThat(authConstraint).hasNext();
            ConstraintDefinition constraint = authConstraint.next();
            assertThat(constraint.getName()).isEqualTo(AUTH_CONSTRAINT);
            assertThatIterable(constraint.getPropertyKeys()).containsExactlyInAnyOrder(AUTH_ID, AUTH_PROVIDER);
            assertThat(authConstraint).isExhausted();
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

    private static Stream<Arguments> beforeUserIdConstraint() {
        return Arrays.stream(UserSecurityGraphComponentVersion.values())
                .filter(version ->
                        version.runtimeSupported() && version.getVersion() < COMMUNITY_SECURITY_50.getVersion())
                .map(Arguments::of);
    }

    private static Stream<Arguments> beforeUserAuth() {
        return Arrays.stream(UserSecurityGraphComponentVersion.values())
                .filter(version ->
                        version.runtimeSupported() && version.getVersion() < COMMUNITY_SECURITY_521.getVersion())
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

    private boolean hasSingleNativeAuthWithUserId(String username) {
        try (Transaction tx = system.beginTransaction(KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED)) {
            Node user = tx.findNode(USER_LABEL, "name", username);
            Relationship authRel = user.getSingleRelationship(HAS_AUTH, OUTGOING);
            if (authRel != null) {
                Node authNode = authRel.getEndNode();
                if (NATIVE_AUTH.equals(authNode.getProperty(AUTH_PROVIDER))) {
                    var userId = user.getProperty(USER_ID, -1);
                    return userId.equals(authNode.getProperty(AUTH_ID, 0));
                }
            }
        }
        return false;
    }

    private static void initUserSecurityComponent(UserSecurityGraphComponentVersion version) throws Exception {
        initUserSecurityComponent(version, Config.defaults());
    }

    private static void initUserSecurityComponent(UserSecurityGraphComponentVersion version, Config config)
            throws Exception {
        KnownCommunitySecurityComponentVersion builder =
                userSecurityGraphComponent.findSecurityGraphComponentVersion(version);
        // initialize schema and then upgrade to version
        inTx(tx -> tx.schema()
                .constraintFor(USER_LABEL)
                .assertPropertyIsUnique("name")
                .create());
        inTx(tx -> builder.upgradeSecurityGraphSchema(tx, FIRST_VALID_COMMUNITY_SECURITY_COMPONENT_VERSION));

        inTx(tx1 -> builder.setupUsers(tx1, config));
        inTx(tx -> builder.setVersionProperty(tx, version.getVersion()));

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
