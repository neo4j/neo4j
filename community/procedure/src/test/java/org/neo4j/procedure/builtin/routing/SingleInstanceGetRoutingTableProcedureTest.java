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
package org.neo4j.procedure.builtin.routing;

import static java.lang.Integer.parseInt;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SERVER_DEFAULTS;
import static org.neo4j.configuration.GraphDatabaseSettings.client_side_router_enforce_for_domains;
import static org.neo4j.configuration.GraphDatabaseSettings.routing_default_router;
import static org.neo4j.dbms.routing.RoutingTableServiceHelpers.ADDRESS_CONTEXT_KEY;
import static org.neo4j.dbms.routing.RoutingTableTTLProvider.ttlFromConfig;
import static org.neo4j.internal.kernel.api.procs.DefaultParameterValue.nullValue;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.inputField;
import static org.neo4j.internal.kernel.api.procs.FieldSignature.outputField;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTInteger;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTMap;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.General.DatabaseUnavailable;
import static org.neo4j.kernel.database.DatabaseIdFactory.from;
import static org.neo4j.values.storable.Values.stringValue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.GraphDatabaseSettings.RoutingMode;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.routing.DatabaseAvailabilityChecker;
import org.neo4j.dbms.routing.DefaultRoutingService;
import org.neo4j.dbms.routing.LocalRoutingTableServiceValidator;
import org.neo4j.dbms.routing.RoutingOption;
import org.neo4j.dbms.routing.RoutingResult;
import org.neo4j.dbms.routing.RoutingService;
import org.neo4j.dbms.routing.SimpleClientRoutingDomainChecker;
import org.neo4j.dbms.routing.SingleAddressRoutingTableProvider;
import org.neo4j.dbms.routing.result.RoutingResultFormat;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.DatabaseReferenceImpl;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.database.DefaultDatabaseResolver;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.database.TestDatabaseReferenceRepository;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public class SingleInstanceGetRoutingTableProcedureTest {
    protected static final NamedDatabaseId ID = from(DEFAULT_DATABASE_NAME, UUID.randomUUID());
    protected static final DatabaseReferenceImpl.Internal REF =
            new DatabaseReferenceImpl.Internal(new NormalizedDatabaseName(ID.name()), ID, true);
    private static final String UNKNOWN_DATABASE_NAME = "unknownDatabaseName";
    private static Context procCtx;

    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest(name = "{1} {0}")
    @MethodSource("routingConfigs")
    private @interface RoutingConfigsTest {}

    @BeforeAll
    static void beforeAll() {
        procCtx = createContext();
    }

    private static Config newConfig(RoutingMode routingMode) {
        return Config.newBuilder()
                .setDefaults(SERVER_DEFAULTS)
                .set(routing_default_router, routingMode)
                .build();
    }

    private static Config newConfig(RoutingMode routingMode, Set<String> enforceClientRoutingDomains) {
        return Config.newBuilder()
                .setDefaults(SERVER_DEFAULTS)
                .set(routing_default_router, routingMode)
                .set(client_side_router_enforce_for_domains, enforceClientRoutingDomains)
                .build();
    }

    private static Stream<Arguments> routingConfigs() {
        // Test cases
        // CSR by default
        // SSR by default and routing context address provided
        // SSR by default and no routing context address provided (older drivers)
        // SSR by default and routing context address provided which is an enforce-csr address
        // SSR by default and routing context address provided which does not match an enforce-csr address
        return Stream.of(
                Arguments.of(newConfig(RoutingMode.CLIENT), null),
                Arguments.of(newConfig(RoutingMode.SERVER), "ssr-hostname:1234"),
                Arguments.of(newConfig(RoutingMode.SERVER), null),
                Arguments.of(newConfig(RoutingMode.SERVER, Set.of("csr-hostname:*")), "csr-hostname:1234"),
                Arguments.of(newConfig(RoutingMode.SERVER, Set.of("csr-hostname:*")), "ssr-hostname:1234"));
    }

    @RoutingConfigsTest
    void shouldHaveCorrectSignature(Config config, String clientAddress) {
        var portRegister = mock(ConnectorPortRegister.class);

        var proc = newProcedure(portRegister, config);

        var signature = proc.signature();

        assertEquals(
                List.of(inputField("context", NTMap), inputField("database", NTString, nullValue(NTString))),
                signature.inputSignature());
        assertEquals(
                List.of(outputField("ttl", NTInteger), outputField("servers", NTList(NTMap))),
                signature.outputSignature());
        assertTrue(signature.systemProcedure());
    }

    @RoutingConfigsTest
    void shouldHaveCorrectNamespace(Config config, String clientAddress) {
        var portRegister = mock(ConnectorPortRegister.class);

        var proc = newProcedure(portRegister, config);

        var name = proc.signature().name();

        assertEquals(new QualifiedName("dbms", "routing", "getRoutingTable"), name);
    }

    @RoutingConfigsTest
    void shouldThrowWhenNoBoltConnectors(Config config, String clientAddress) {
        // given
        config = newConfig(config, Duration.ofSeconds(123), null);
        config.set(BoltConnector.enabled, false);

        var portRegister = mock(ConnectorPortRegister.class);
        var proc = newProcedure(portRegister, config);
        var input = new AnyValue[] {MapValue.EMPTY, stringValue(DEFAULT_DATABASE_NAME)};

        // when
        var exception = assertThrows(ProcedureException.class, () -> proc.apply(procCtx, input, null));

        // then
        assertEquals(Status.Procedure.ProcedureCallFailed, exception.status());
        assertThat(exception)
                .hasMessageEndingWith(" Please update your configuration for '" + BoltConnector.enabled.name() + "'");
    }

    @RoutingConfigsTest
    void shouldReturnRoutingTable(Config config, String clientAddress) throws Exception {
        var portRegister = mock(ConnectorPortRegister.class);
        config = newConfig(config, Duration.ofMinutes(42), new SocketAddress("neo4j.com", 7687));

        var proc = newProcedure(portRegister, config);

        var result = invoke(proc, clientAddress);

        assertEquals(Duration.ofMinutes(42).toMillis(), result.ttlMillis());

        var address = clientAddress != null ? socketAddressFor(clientAddress) : new SocketAddress("neo4j.com", 7687);
        assertEquals(singletonList(address), result.readEndpoints());
        assertEquals(expectedWriters(address), result.writeEndpoints());
        assertEquals(singletonList(address), result.routeEndpoints());
    }

    private SocketAddress socketAddressFor(String clientAddress) {
        var addressParts = clientAddress.split(":");
        return new SocketAddress(addressParts[0], addressParts.length == 1 ? 7687 : parseInt(addressParts[1]));
    }

    @RoutingConfigsTest
    void shouldThrowWhenDatabaseDoesNotExist(Config config, String clientAddress) {
        var portRegister = mock(ConnectorPortRegister.class);

        var databasebAvailabilityChecker = new TestDatabaseAvailabilityChecker();
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed();

        var procedure = newProcedure(
                databasebAvailabilityChecker,
                databaseReferenceRepo,
                portRegister,
                config,
                NullLogProvider.getInstance());

        var error = assertThrows(
                ProcedureException.class, () -> invoke(Map.of(), procedure, clientAddress, UNKNOWN_DATABASE_NAME));
        assertEquals(DatabaseNotFound, error.status());
    }

    @RoutingConfigsTest
    void shouldThrowWhenDatabaseIsStopped(Config config, String clientAddress) {
        var portRegister = mock(ConnectorPortRegister.class);
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, false);

        var procedure = newProcedure(
                databaseAvailabilityChecker,
                databaseReferenceRepo,
                portRegister,
                config,
                NullLogProvider.getInstance());

        var error = assertThrows(ProcedureException.class, () -> invoke(procedure, clientAddress));
        assertEquals(DatabaseUnavailable, error.status());
    }

    @ParameterizedTest
    @EnumSource(value = RoutingMode.class)
    void shouldThrowWhenAddressCtxIsPresentButEmpty(RoutingMode routingMode) {
        // given
        var ctxContents = new MapValueBuilder();
        ctxContents.add(ADDRESS_CONTEXT_KEY, Values.EMPTY_STRING);
        var ctx = ctxContents.build();

        var portRegister = mock(ConnectorPortRegister.class);
        var config = newConfig(
                Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), new SocketAddress("neo4j.com", 7687));
        config.set(routing_default_router, routingMode);

        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedMessage =
                "An address key is included in the query string provided to the GetRoutingTableProcedure, but its"
                        + " value could not be parsed.";

        // when/then
        assertThrows(ProcedureException.class, () -> invoke(procedure, ID, ctx), expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(value = RoutingMode.class)
    void shouldThrowWhenHostCtxIsInvalid(RoutingMode routingMode) {
        // given
        var ctxContents = new MapValueBuilder();
        ctxContents.add(ADDRESS_CONTEXT_KEY, Values.stringValue("not a socket address"));
        var ctx = ctxContents.build();

        var config = newConfig(
                Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), new SocketAddress("neo4j.com", 7687));
        config.set(routing_default_router, routingMode);

        var portRegister = mock(ConnectorPortRegister.class);
        var logProvider = new AssertableLogProvider();

        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedMessage =
                "An address key is included in the query string provided to the GetRoutingTableProcedure, but its"
                        + " value could not be parsed.";

        // when/then
        assertThrows(ProcedureException.class, () -> invoke(procedure, ID, ctx), expectedMessage);
    }

    @RoutingConfigsTest
    void shouldUseClientProvidedHostAndAdvertisedPortAsAdvertisedAddress(Config config, String clientAddress)
            throws Exception {
        // given
        var advertisedBoldPort = 8776;
        var defaultBoltPort = 7687;
        var advertisedBoltAddress = new SocketAddress("neo4j.com", advertisedBoldPort);
        config.set(BoltConnector.advertised_address, advertisedBoltAddress);
        var clientProvidedHost =
                clientAddress == null ? "mydomain.com" : clientAddress.split(":")[0];
        var ctxContents = new MapValueBuilder();
        ctxContents.add(ADDRESS_CONTEXT_KEY, Values.stringValue(clientProvidedHost));
        var ctx = ctxContents.build();

        var portRegister = mock(ConnectorPortRegister.class);
        when(portRegister.getLocalAddress(ConnectorType.BOLT))
                .thenReturn(new HostnamePort("neo4j.com", advertisedBoldPort));
        var logProvider = new AssertableLogProvider();

        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedAddress = new SocketAddress(clientProvidedHost, defaultBoltPort);

        // when
        var result = invoke(procedure, ID, ctx);

        // then
        assertEquals(singletonList(expectedAddress), result.readEndpoints());
        assertEquals(expectedWriters(expectedAddress), result.writeEndpoints());
        assertEquals(singletonList(expectedAddress), result.routeEndpoints());
        assertEquals(singletonList(expectedAddress), result.routeEndpoints());
    }

    /**
     * This tests a very specific non-documented behaviour - if the client provided address has the port explicitly set to zero then the returned routing table
     * uses the advertised address. We make use of this behaviour in some internal tests. In "real life" we should never encounter a client provided address
     * specifically set to zero so there's no particular reason to change this.
     */
    @RoutingConfigsTest
    void shouldUseConfiguredAdvertisedAddressIfClientProvidedPortIsZero(Config config, String clientAddress)
            throws Exception {
        // given
        var advertisedBoldPort = 8776;
        var clientProvidedPort = 0;
        var advertisedBoltAddress = new SocketAddress("neo4j.com", advertisedBoldPort);
        config.set(BoltConnector.advertised_address, advertisedBoltAddress);
        var clientProvidedHost =
                clientAddress == null ? "mydomain.com" : clientAddress.split(":")[0];
        var clientProvidedHostPortStr = String.format("%s:%d", clientProvidedHost, clientProvidedPort);

        var portRegister = mock(ConnectorPortRegister.class);
        when(portRegister.getLocalAddress(ConnectorType.BOLT))
                .thenReturn(new HostnamePort("neo4j.com", advertisedBoldPort));

        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedAddress = advertisedBoltAddress;

        // when
        RoutingResult result = invoke(procedure, clientProvidedHostPortStr);

        // then
        assertEquals(singletonList(expectedAddress), result.readEndpoints());
        assertEquals(expectedWriters(expectedAddress), result.writeEndpoints());
        assertEquals(singletonList(expectedAddress), result.routeEndpoints());
    }

    @ParameterizedTest
    @EnumSource(value = RoutingMode.class)
    void shouldThrowIfClientProvidedPortIsNegative(RoutingMode routingMode) {
        // given
        var advertisedBoldPort = 8776;
        var clientProvidedPort = -5;
        var advertisedBoltAddress = new SocketAddress("neo4j.com", advertisedBoldPort);
        var clientProvidedHostPortStr = String.format("%s:%d", "my.neo4j-service.com", clientProvidedPort);

        var ctxContents = new MapValueBuilder();
        ctxContents.add(ADDRESS_CONTEXT_KEY, Values.stringValue(clientProvidedHostPortStr));
        var ctx = ctxContents.build();

        var portRegister = mock(ConnectorPortRegister.class);
        when(portRegister.getLocalAddress(ConnectorType.BOLT))
                .thenReturn(new HostnamePort("neo4j.com", advertisedBoldPort));
        var config = newConfig(Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), advertisedBoltAddress);
        config.set(routing_default_router, routingMode);
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedMessage =
                "An address key is included in the query string provided to the GetRoutingTableProcedure, but its"
                        + " value could not be parsed.";

        // when
        assertThrows(ProcedureException.class, () -> invoke(procedure, ID, ctx), expectedMessage);
    }

    @ParameterizedTest
    @EnumSource(value = RoutingMode.class)
    void shouldThrowIfClientProvidedPortIsNotANumber(RoutingMode routingMode) {
        // given
        var advertisedBoldPort = 8776;
        var clientProvidedPort = "bolt";
        var advertisedBoltAddress = new SocketAddress("neo4j.com", advertisedBoldPort);
        var clientProvidedHostPortStr = String.format("%s:%s", "my.neo4j-service.com", clientProvidedPort);

        var ctxContents = new MapValueBuilder();
        ctxContents.add(ADDRESS_CONTEXT_KEY, Values.stringValue(clientProvidedHostPortStr));
        var ctx = ctxContents.build();

        var portRegister = mock(ConnectorPortRegister.class);
        when(portRegister.getLocalAddress(ConnectorType.BOLT))
                .thenReturn(new HostnamePort("neo4j.com", advertisedBoldPort));
        var config = newConfig(Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), advertisedBoltAddress);
        config.set(routing_default_router, routingMode);
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedMessage =
                "An address key is included in the query string provided to the GetRoutingTableProcedure, but its"
                        + " value could not be parsed.";

        // when
        assertThrows(ProcedureException.class, () -> invoke(procedure, ID, ctx), expectedMessage);
    }

    @Test
    void whenServerRoutingEnabledShouldUseClientProvidedHostAndDefaultPortAsAdvertisedAddress() throws Exception {
        // given
        var advertisedBoldPort = 8776;
        var advertisedBoltAddress = new SocketAddress("neo4j.com", advertisedBoldPort);
        var clientProvidedHost = "my.neo4j-service.com";

        var config = newConfig(Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), advertisedBoltAddress);
        config.set(GraphDatabaseSettings.routing_default_router, GraphDatabaseSettings.RoutingMode.SERVER);

        var portRegister = mock(ConnectorPortRegister.class);
        when(portRegister.getLocalAddress(ConnectorType.BOLT))
                .thenReturn(new HostnamePort("neo4j.com", advertisedBoldPort));

        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedAddress = new SocketAddress(clientProvidedHost, 7687);

        // when
        RoutingResult result = invoke(procedure, clientProvidedHost);

        // then
        assertEquals(singletonList(expectedAddress), result.readEndpoints());
        assertEquals(expectedWriters(expectedAddress), result.writeEndpoints());
        assertEquals(singletonList(expectedAddress), result.routeEndpoints());
    }

    private static MapValue newClientContext(String clientAddress, Map<String, ?> clientContext) {
        MapValue mapValue = ValueUtils.asMapValue(clientContext);
        if (clientAddress == null) {
            return mapValue;
        }
        return mapValue.updatedWith(ADDRESS_CONTEXT_KEY, Values.stringValue(clientAddress));
    }

    private RoutingResult invoke(GetRoutingTableProcedure procedure, String clientAddress) throws ProcedureException {
        return invoke(Map.of(), procedure, clientAddress, ID.name());
    }

    private RoutingResult invoke(
            Map<String, ?> clientContext, GetRoutingTableProcedure procedure, String clientAddress, String databaseName)
            throws ProcedureException {
        var ctx = newClientContext(clientAddress, clientContext);
        var rawResult = procedure.apply(procCtx, new AnyValue[] {ctx, Values.stringValue(databaseName)}, null);
        var result = RoutingResultFormat.parse(rawResult.next());
        assertThat(rawResult.hasNext())
                .as("Routing procedure should only ever return a single row")
                .isFalse();
        return result;
    }

    @ParameterizedTest
    @EnumSource(value = RoutingMode.class)
    void shouldUseClientProvidedHostAndPortAsAdvertisedAddress(RoutingMode routingMode) throws Exception {
        // given
        var advertisedBoltAddress = new SocketAddress("neo4j.com", 7687);
        var clientProvidedPort = 8888;
        var clientProvidedHost = "my.neo4j-service.com";
        var clientProvidedHostPortStr = String.format("%s:%d", clientProvidedHost, clientProvidedPort);
        var config = newConfig(Config.defaults(SERVER_DEFAULTS), Duration.ofSeconds(100), advertisedBoltAddress);
        config.set(routing_default_router, routingMode);

        var portRegister = mock(ConnectorPortRegister.class);
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        var logProvider = new AssertableLogProvider();

        var procedure =
                newProcedure(databaseAvailabilityChecker, databaseReferenceRepo, portRegister, config, logProvider);
        var expectedAddress = new SocketAddress(clientProvidedHost, clientProvidedPort);

        // when
        RoutingResult result = invoke(procedure, clientProvidedHostPortStr);

        // then
        assertEquals(singletonList(expectedAddress), result.readEndpoints());
        assertEquals(expectedWriters(expectedAddress), result.writeEndpoints());
        assertEquals(singletonList(expectedAddress), result.routeEndpoints());
    }

    protected GetRoutingTableProcedure newProcedure(
            DatabaseAvailabilityChecker databaseAvailabilityChecker,
            DatabaseReferenceRepository databaseReferenceRepo,
            ConnectorPortRegister portRegister,
            Config config,
            InternalLogProvider logProvider) {
        var clientRoutingDomainChecker = SimpleClientRoutingDomainChecker.fromConfig(config, logProvider);
        var defaultDatabaseResolver = mock(DefaultDatabaseResolver.class);

        LocalRoutingTableServiceValidator validator =
                new LocalRoutingTableServiceValidator(databaseAvailabilityChecker);
        SingleAddressRoutingTableProvider routingTableProvider = new SingleAddressRoutingTableProvider(
                portRegister, RoutingOption.ROUTE_WRITE_AND_READ, config, logProvider, ttlFromConfig(config));

        RoutingService routingService = new DefaultRoutingService(
                logProvider,
                validator,
                routingTableProvider,
                routingTableProvider,
                clientRoutingDomainChecker,
                config,
                () -> false,
                defaultDatabaseResolver,
                databaseReferenceRepo,
                true);

        var routingTableProcedures = GetRoutingTableProcedure.from(routingService, logProvider);
        return routingTableProcedures.current();
    }

    protected List<SocketAddress> expectedWriters(SocketAddress selfAddress) {
        return singletonList(selfAddress);
    }

    private GetRoutingTableProcedure newProcedure(ConnectorPortRegister portRegister, Config config) {
        var databaseReferenceRepo = new TestDatabaseReferenceRepository.Fixed(REF);
        var databaseAvailabilityChecker = new TestDatabaseAvailabilityChecker().withDatabase(REF, true);
        return newProcedure(
                databaseAvailabilityChecker,
                databaseReferenceRepo,
                portRegister,
                config,
                NullLogProvider.getInstance());
    }

    private static Config newConfig(Config config, Duration routingTtl, SocketAddress boltAddress) {
        var builder = Config.newBuilder().fromConfig(config);
        if (routingTtl != null) {
            builder.set(GraphDatabaseSettings.routing_ttl, routingTtl);
        }
        if (boltAddress != null) {
            builder.set(BoltConnector.enabled, true);
            builder.set(BoltConnector.listen_address, boltAddress);
            builder.set(BoltConnector.advertised_address, boltAddress);
        }
        return builder.build();
    }

    private static RoutingResult invoke(GetRoutingTableProcedure proc, NamedDatabaseId databaseName, MapValue context)
            throws ProcedureException {
        return invoke(proc, databaseName.name(), context);
    }

    private static RoutingResult invoke(GetRoutingTableProcedure proc, String databaseName, MapValue context)
            throws ProcedureException {
        var input = new AnyValue[] {context, stringValue(databaseName)};
        var result = proc.apply(procCtx, input, null);
        var routingResultAndTtl = RoutingResultFormat.parse(result.next());
        assertFalse(result.hasNext(), "Routing procedure should only return a single row");
        return routingResultAndTtl;
    }

    private static Context createContext() {
        var ctx = mock(Context.class);
        var securityContext = mock(SecurityContext.class);
        var subject = mock(AuthSubject.class);
        when(ctx.securityContext()).thenReturn(securityContext);
        when(securityContext.subject()).thenReturn(subject);
        when(subject.executingUser()).thenReturn("user");
        return ctx;
    }
}
