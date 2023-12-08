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
package org.neo4j.kernel.builtinprocs;

import static org.apache.commons.lang3.ArrayUtils.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.capabilities.Type.BOOLEAN;
import static org.neo4j.capabilities.Type.DOUBLE;
import static org.neo4j.capabilities.Type.INTEGER;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureName;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.neo4j.annotations.Public;
import org.neo4j.capabilities.CapabilitiesRegistry;
import org.neo4j.capabilities.CapabilitiesSettings;
import org.neo4j.capabilities.Capability;
import org.neo4j.capabilities.CapabilityDeclaration;
import org.neo4j.capabilities.CapabilityProvider;
import org.neo4j.capabilities.CapabilityProviderContext;
import org.neo4j.capabilities.DBMSCapabilities;
import org.neo4j.capabilities.Name;
import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;

class BuiltInDbmsProceduresIT extends KernelIntegrationTest {
    @Test
    void listConfig() throws Exception {
        // When
        List<AnyValue[]> config = callListConfig("");
        List<String> names =
                config.stream().map(o -> ((TextValue) o[0]).stringValue()).collect(Collectors.toList());

        // The size of the config is not fixed so just make sure it's the right magnitude
        assertTrue(names.size() > 10);

        assertThat(names).contains(GraphDatabaseSettings.db_format.name());

        // Should not contain "internal.*" configs
        assertEquals(0, names.stream().filter(n -> n.startsWith("internal")).count());
    }

    @Test
    void listConfigWithASpecificConfigName() throws Exception {
        // When
        List<AnyValue[]> config = callListConfig(GraphDatabaseSettings.strict_config_validation.name());

        assertEquals(1, config.size());
        assertThat(config.get(0)).isEqualTo(new AnyValue[] {
            stringValue("server.config.strict_validation.enabled"),
            stringValue("A strict configuration validation will prevent the database from starting up if unknown "
                    + "configuration options are specified in the neo4j settings namespace (such as dbms., cypher., etc) "
                    + "or if settings are declared multiple times."),
            stringValue(TRUE),
            Values.FALSE,
            stringValue(TRUE),
            stringValue(TRUE),
            Values.FALSE,
            stringValue("A boolean.")
        });
    }

    @Test
    void listClientConfig() throws Exception {
        QualifiedName procedureName = procedureName("dbms", "clientConfig");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> config = asList(callResult);
            assertEquals(config.size(), 4);

            assertEquals(stringValue("browser.post_connect_cmd"), config.get(0)[0]);
            assertEquals(stringValue("browser.remote_content_hostname_whitelist"), config.get(1)[0]);
            assertEquals(stringValue("client.allow_telemetry"), config.get(2)[0]);
            assertEquals(stringValue("dbms.security.auth_enabled"), config.get(3)[0]);
        }
    }

    @Test
    void durationAlwaysListedWithUnit() throws Exception {
        // When
        List<AnyValue[]> config = callListConfig(GraphDatabaseSettings.transaction_timeout.name());

        assertEquals(1, config.size());
        assertThat(config.get(0)).isEqualTo(new AnyValue[] {
            stringValue("db.transaction.timeout"),
            stringValue("The maximum time interval of a transaction within which it should be completed."),
            stringValue("0s"),
            Values.TRUE,
            stringValue("0s"),
            stringValue("0s"),
            Values.FALSE,
            stringValue("A duration (Valid units are: `ns`, `μs`, `ms`, `s`, `m`, `h` and `d`; default unit is `s`)."),
        });
    }

    @Test
    void listDynamicSetting() throws KernelException {
        List<AnyValue[]> config = callListConfig(GraphDatabaseSettings.check_point_iops_limit.name());

        assertEquals(1, config.size());
        assertTrue(((BooleanValue) config.get(0)[3]).booleanValue());
    }

    @Test
    void listNotDynamicSetting() throws KernelException {
        List<AnyValue[]> config = callListConfig(GraphDatabaseSettings.data_directory.name());

        assertEquals(1, config.size());
        assertFalse(((BooleanValue) config.get(0)[3]).booleanValue());
    }

    @Test
    void listCapabilities() throws KernelException {
        QualifiedName procedureName = procedureName("dbms", "listCapabilities");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> capabilities = asList(callResult);
            List<String> capabilityNames = capabilities.stream()
                    .map(c -> ((TextValue) c[0]).stringValue())
                    .collect(Collectors.toList());

            assertThat(capabilityNames)
                    .containsExactlyInAnyOrder(
                            TestCapabilities.my_custom_capability.name().fullName(),
                            TestCapabilities.my_dynamic_capability.name().fullName());
        }
    }

    @Test
    void listCapabilitiesShouldNotReturnBlocked() throws KernelException {
        // set blocked capabilities
        Config config = dependencyResolver.resolveDependency(Config.class);
        config.set(CapabilitiesSettings.dbms_capabilities_blocked, List.of("my.**"));

        QualifiedName procedureName = procedureName("dbms", "listCapabilities");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> capabilities = asList(callResult);
            List<String> capabilityNames = capabilities.stream()
                    .map(c -> ((TextValue) c[0]).stringValue())
                    .collect(Collectors.toList());

            assertThat(capabilityNames)
                    .doesNotContain(TestCapabilities.my_custom_capability.name().fullName());
        }
    }

    @Test
    void listCapabilitiesShouldReturnDynamicValues() throws KernelException {
        QualifiedName procedureName = procedureName("dbms", "listCapabilities");
        var procs = procs();
        int procedureId = procs.procedureGet(procedureName).id();
        try (var statement = kernelTransaction.acquireStatement()) {

            // first call
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> capabilities = asList(callResult);

            // should return false
            assertThat(capabilities).contains(new AnyValue[] {
                Values.stringValue(TestCapabilities.my_dynamic_capability.name().fullName()),
                Values.stringValue(TestCapabilities.my_dynamic_capability.description()),
                Values.booleanValue(false)
            });
        }

        try (var txc = db.beginTx()) {
            txc.createNode(label("my_dynamic_capability"));
            txc.commit();
        }

        // second call
        procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            var callResult = procs().procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            var capabilities = asList(callResult);

            // should return true
            assertThat(capabilities).contains(new AnyValue[] {
                Values.stringValue(TestCapabilities.my_dynamic_capability.name().fullName()),
                Values.stringValue(TestCapabilities.my_dynamic_capability.description()),
                Values.booleanValue(true)
            });
        }
    }

    @Test
    void listAllCapabilities() throws KernelException {
        QualifiedName procedureName = procedureName("dbms", "listAllCapabilities");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> capabilities = asList(callResult);
            List<String> capabilityNames = capabilities.stream()
                    .map(c -> ((TextValue) c[0]).stringValue())
                    .collect(Collectors.toList());

            assertThat(capabilityNames)
                    .containsExactlyInAnyOrder(
                            DBMSCapabilities.dbms_instance_version.name().fullName(),
                            DBMSCapabilities.dbms_instance_kernel_version.name().fullName(),
                            DBMSCapabilities.dbms_instance_edition.name().fullName(),
                            DBMSCapabilities.dbms_instance_operational_mode
                                    .name()
                                    .fullName(),
                            TestCapabilities.my_custom_capability.name().fullName(),
                            TestCapabilities.my_internal_capability.name().fullName(),
                            TestCapabilities.my_dynamic_capability.name().fullName());
        }
    }

    @Test
    void listAllCapabilitiesShouldNotReturnBlocked() throws KernelException {
        // set blocked capabilities
        Config config = dependencyResolver.resolveDependency(Config.class);
        config.set(CapabilitiesSettings.dbms_capabilities_blocked, List.of("my.custom.**"));

        QualifiedName procedureName = procedureName("dbms", "listAllCapabilities");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {

            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult =
                    procs.procedureCallDbms(procedureId, new AnyValue[] {}, ProcedureCallContext.EMPTY);
            List<AnyValue[]> capabilities = asList(callResult);
            List<String> capabilityNames = capabilities.stream()
                    .map(c -> ((TextValue) c[0]).stringValue())
                    .collect(Collectors.toList());

            assertThat(capabilityNames)
                    .containsExactlyInAnyOrder(
                            DBMSCapabilities.dbms_instance_version.name().fullName(),
                            DBMSCapabilities.dbms_instance_kernel_version.name().fullName(),
                            DBMSCapabilities.dbms_instance_edition.name().fullName(),
                            DBMSCapabilities.dbms_instance_operational_mode
                                    .name()
                                    .fullName(),
                            TestCapabilities.my_dynamic_capability.name().fullName(),
                            TestCapabilities.my_internal_capability.name().fullName());
        }
    }

    private List<AnyValue[]> callListConfig(String searchString) throws KernelException {
        QualifiedName procedureName = procedureName("dbms", "listConfig");
        Procedures procs = procs();
        try (var statement = kernelTransaction.acquireStatement()) {
            int procedureId = procs.procedureGet(procedureName).id();
            RawIterator<AnyValue[], ProcedureException> callResult = procs.procedureCallDbms(
                    procedureId, toArray(stringValue(searchString)), ProcedureCallContext.EMPTY);
            return asList(callResult);
        }
    }

    public static class TestCapabilities implements CapabilityDeclaration, CapabilityProvider {
        @Public
        @Description("my custom capability")
        public static final Capability<Integer> my_custom_capability =
                new Capability<>(Name.of("my.custom.capability"), INTEGER);

        @Description("my internal capability")
        public static final Capability<Double> my_internal_capability =
                new Capability<>(Name.of("my.internal.capability"), DOUBLE);

        @Public
        @Description("my dynamic capability")
        public static final Capability<Boolean> my_dynamic_capability =
                new Capability<>(Name.of("my.dynamic.capability"), BOOLEAN);

        @Override
        public String namespace() {
            return "my";
        }

        @Override
        public void register(CapabilityProviderContext ctx, CapabilitiesRegistry registry) {
            registry.set(my_custom_capability, 123);
            registry.supply(my_internal_capability, () -> 3.0 + 4.5);
            registry.supply(my_dynamic_capability, () -> {
                var dbms = ctx.dbms();
                var db = dbms.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

                try (var txc = db.beginTx();
                        var it = txc.findNodes(label("my_dynamic_capability"))) {
                    return it.stream().findAny().isPresent();
                }
            });
        }
    }
}
