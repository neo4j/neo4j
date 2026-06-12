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

import static org.neo4j.configuration.helpers.CypherVersionClassification.isExperimental;
import static org.neo4j.configuration.helpers.CypherVersionClassification.shortName;
import static org.neo4j.internal.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTList;
import static org.neo4j.internal.kernel.api.procs.Neo4jTypes.NTString;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.neo4j.capabilities.CapabilitiesService;
import org.neo4j.capabilities.Name;
import org.neo4j.collection.ResourceRawIterator;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings.CypherVersion;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.ResourceMonitor;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.Version;
import org.neo4j.procedure.Mode;
import org.neo4j.service.Services;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * This procedure lists "components" and their version. While components are currently hard-coded, it is intended that this implementation will be replaced once
 * a clean system for component assembly exists where we could dynamically get a list of which components are loaded and what versions of them.
 * <p>
 * This way, it works as a general mechanism into which capabilities a given Neo4j system has, and which version of those components are in use.
 * <p>
 * This would include things like Kernel, Storage Engine, Virtual Graph, Bolt protocol versions et cetera.
 * <p>
 * The versions returned from `dbms.components()` for the "Neo4j Kernel" component depend on several factors
 * <p>
 * 1. If a value has been explicitly set for the `internal.dbms.custom_kernel_version` configuration then we return that.
 * <p>
 * 2. Else, if a custom system property `internal.neo4j.custom.version` has been set, then we return that.
 * <p>
 * 3. Else, we return the current version.
 */
public class ListComponentsProcedure extends CallableProcedure.BasicProcedure {
    private static final String SKIP_CYPHER_VERSION = "internal.components.cypher.skip";

    public static final String NAME_COLUMN = "name";
    public static final String VERSIONS_COLUMN = "versions";
    public static final String EDITION_COLUMN = "edition";
    public static final String KERNEL_COMPONENT_NAME = "Neo4j Kernel";
    public static final String VIRTUAL_GRAPH_COMPONENT_NAME = "Virtual Graph";

    private static final TextValue NEO4J_KERNEL = utf8Value(KERNEL_COMPONENT_NAME);
    private static final TextValue VIRTUAL_GRAPH = utf8Value(VIRTUAL_GRAPH_COMPONENT_NAME);
    private static final TextValue CYPHER = utf8Value("Cypher");
    private static final CypherVersion[] ALL_CYPHER_VERSIONS = CypherVersion.values();
    private static final ListValue CYPHER_VERSIONS_EXCL_EXPERIMENTAL = cypherVersions(false);
    private static final ListValue CYPHER_VERSIONS_INCL_EXPERIMENTAL = cypherVersions(true);

    private volatile List<AnyValue[]> components;

    public ListComponentsProcedure(QualifiedName name) {
        super(procedureSignature(name)
                .out(NAME_COLUMN, NTString, "The name of the component.")
                // Since Bolt, Cypher and other components support multiple versions
                // at the same time, list of versions rather than single version.
                .out(VERSIONS_COLUMN, NTList(NTString), "The installed versions of the component.")
                .out(EDITION_COLUMN, NTString, "The Neo4j edition of the DBMS.")
                .mode(Mode.DBMS)
                .description("List DBMS components and their versions.")
                .systemProcedure()
                .build());
    }

    @Override
    public ResourceRawIterator<AnyValue[], ProcedureException> apply(
            Context ctx, AnyValue[] input, ResourceMonitor resourceMonitor) throws ProcedureException {

        var dependencyResolver = ctx.dependencyResolver();
        return asRawIterator(getComponents(
                ctx.graphDatabaseAPI().dbmsInfo(),
                dependencyResolver.resolveDependency(Configuration.class),
                dependencyResolver.resolveDependency(CapabilitiesService.class)));
    }

    private List<AnyValue[]> getComponents(
            DbmsInfo dbmsInfo, Configuration configuration, CapabilitiesService capabilitiesService) {

        var result = this.components;
        if (result == null) {
            synchronized (this) {
                result = this.components;
                if (result == null) {
                    this.components = createComponentsList(
                            Version.getNeo4jVersion(), dbmsInfo.edition.toString(), configuration, capabilitiesService);
                    result = this.components;
                }
            }
        }
        return result;
    }

    private static List<AnyValue[]> createComponentsList(
            String neo4jVersion,
            String neo4jEdition,
            Configuration configuration,
            CapabilitiesService capabilitiesService) {

        var customVersionConfig = configuration.get(GraphDatabaseInternalSettings.custom_kernel_version);
        var version = customVersionConfig != null ? stringValue(customVersionConfig) : stringValue(neo4jVersion);
        var edition = stringValue(neo4jEdition);

        List<AnyValue[]> components = new ArrayList<>(3);
        components.add(new AnyValue[] {NEO4J_KERNEL, VirtualValues.list(version), edition});

        if (!Boolean.getBoolean(SKIP_CYPHER_VERSION)) {
            var cypherExperimentalVersionsEnabled =
                    configuration.get(GraphDatabaseInternalSettings.enable_experimental_cypher_versions);
            var cypherVersions = Boolean.TRUE.equals(cypherExperimentalVersionsEnabled)
                    ? CYPHER_VERSIONS_INCL_EXPERIMENTAL
                    : CYPHER_VERSIONS_EXCL_EXPERIMENTAL;
            components.add(new AnyValue[] {CYPHER, cypherVersions, EMPTY_STRING});
        }

        if (getVirtualGraphEnabledSetting(configuration)) {
            var geVersion = Objects.requireNonNullElse(
                    (String) capabilitiesService.get(Name.of("virtual_graph.version")), "n/a");
            components.add(new AnyValue[] {VIRTUAL_GRAPH, VirtualValues.list(stringValue(geVersion)), stringValue("")});
        }

        return components;
    }

    static boolean getVirtualGraphEnabledSetting(Configuration configuration) {
        // Use service loader to not compile time depend on enterprise settings (safe)
        var settings = Services.load(
                SettingsDeclaration.class,
                "EnterpriseEditionInternalSettings",
                p -> p.getClass().getSimpleName());
        if (settings.isEmpty()) {
            return false;
        }
        try {
            // Retrieve the settings fields via reflection (less safe, but no other way unless do pull the enterprise
            // module into community, and we don't want).
            // Ok from performance, only done on first call and later on cached in #components.
            @SuppressWarnings("unchecked")
            var virtualGraphEnabled = (Setting<Boolean>)
                    settings.get().getClass().getField("virtual_graph_enabled").get(null);
            return Boolean.TRUE.equals(configuration.get(virtualGraphEnabled));
        } catch (NoSuchFieldException | IllegalAccessException e) {
            return false;
        }
    }

    private static ListValue cypherVersions(boolean includeExperimental) {
        return VirtualValues.fromList(Arrays.stream(ALL_CYPHER_VERSIONS)
                .filter(v -> includeExperimental || !isExperimental(v))
                .map(v -> stringValue(shortName(v)))
                .collect(Collectors.toList()));
    }
}
