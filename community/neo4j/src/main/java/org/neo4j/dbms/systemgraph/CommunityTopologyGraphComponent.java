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
package org.neo4j.dbms.systemgraph;

import static org.neo4j.dbms.database.ComponentVersion.COMMUNITY_TOPOLOGY_GRAPH_COMPONENT;
import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;
import static org.neo4j.dbms.systemgraph.CommunityTopologyGraphVersion.LATEST_COMMUNITY_TOPOLOGY_VERSION;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.AbstractSystemGraphComponent;
import org.neo4j.dbms.database.KnownSystemComponentVersions;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponentWithVersion;
import org.neo4j.dbms.systemgraph.versions.CommunityTopologyComponentVersion_0_44;
import org.neo4j.dbms.systemgraph.versions.CommunityTopologyComponentVersion_1_50;
import org.neo4j.dbms.systemgraph.versions.CommunityTopologyComponentVersion_2_58;
import org.neo4j.dbms.systemgraph.versions.KnownCommunityTopologyComponentVersion;
import org.neo4j.dbms.systemgraph.versions.NoCommunityTopologyComponentVersion;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * This component handles the community parts of the topology graph:
 * - the database access property
 */
public class CommunityTopologyGraphComponent extends AbstractSystemGraphComponent
        implements SystemGraphComponentWithVersion {
    private final InternalLog log;
    private final KnownSystemComponentVersions<KnownCommunityTopologyComponentVersion>
            knownCommunityTopologyComponentVersions =
                    new KnownSystemComponentVersions<>(new NoCommunityTopologyComponentVersion());

    public CommunityTopologyGraphComponent(Config config, InternalLogProvider logProvider) {
        super(config);
        this.log = logProvider.getLog(getClass());

        KnownCommunityTopologyComponentVersion version0 = new CommunityTopologyComponentVersion_0_44();
        KnownCommunityTopologyComponentVersion version1 = new CommunityTopologyComponentVersion_1_50();
        KnownCommunityTopologyComponentVersion version2 = new CommunityTopologyComponentVersion_2_58(version1);
        knownCommunityTopologyComponentVersions.add(version0);
        knownCommunityTopologyComponentVersions.add(version1);
        knownCommunityTopologyComponentVersions.add(version2);
    }

    @Override
    public Name componentName() {
        return COMMUNITY_TOPOLOGY_GRAPH_COMPONENT;
    }

    @Override
    public int getLatestSupportedVersion() {
        return LATEST_COMMUNITY_TOPOLOGY_VERSION;
    }

    @Override
    public Status detect(Transaction tx) {
        return knownCommunityTopologyComponentVersions
                .detectCurrentComponentVersion(tx)
                .getStatus(config);
    }

    @Override
    protected void initializeSystemGraphModel(Transaction tx, GraphDatabaseService systemDb) {
        KnownCommunityTopologyComponentVersion latest =
                knownCommunityTopologyComponentVersions.latestComponentVersion();
        latest.setVersionProperty(tx, latest.version);
        latest.initializeTopologyGraph(tx);
    }

    @Override
    public void upgradeToCurrent(GraphDatabaseService system) throws Exception {
        SystemGraphComponent.executeWithFullAccess(system, tx -> {
            KnownCommunityTopologyComponentVersion currentVersion =
                    knownCommunityTopologyComponentVersions.detectCurrentComponentVersion(tx);
            log.debug(String.format(
                    "Trying to upgrade component '%s' with version %d and status %s to latest version",
                    COMMUNITY_TOPOLOGY_GRAPH_COMPONENT, currentVersion.version, currentVersion.getStatus(config)));
            if (currentVersion.version == UNKNOWN_VERSION) {
                log.debug("The current version does not have a community topology graph, doing a full initialization");
                initializeSystemGraphModel(tx, system);
            } else {
                if (currentVersion.migrationSupported()) {
                    log.info(String.format(
                            "Upgrading '%s' component to latest version", COMMUNITY_TOPOLOGY_GRAPH_COMPONENT));
                    knownCommunityTopologyComponentVersions
                            .latestComponentVersion()
                            .upgradeTopologyGraph(tx, currentVersion.version);
                } else {
                    throw currentVersion.unsupported();
                }
            }
        });
    }
}
