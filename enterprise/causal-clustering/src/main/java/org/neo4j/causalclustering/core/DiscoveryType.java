/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;

import org.neo4j.causalclustering.discovery.DnsHostnameResolver;
import org.neo4j.causalclustering.discovery.DomainNameResolverImpl;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.causalclustering.discovery.KubernetesResolver;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.SrvHostnameResolver;
import org.neo4j.causalclustering.discovery.SrvRecordResolverImpl;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.internal.LogService;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_label_selector;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.kubernetes_service_port_name;

public enum DiscoveryType
{
    DNS( ( logService, conf ) -> DnsHostnameResolver.resolver( logService, new DomainNameResolverImpl(), conf ),
            initial_discovery_members ),

    LIST( ( logService, conf ) -> NoOpHostnameResolver.resolver( conf ),
            initial_discovery_members ),

    SRV( ( logService, conf ) -> SrvHostnameResolver.resolver( logService, new SrvRecordResolverImpl(), conf ),
            initial_discovery_members ),

    K8S( KubernetesResolver::resolver,
            kubernetes_label_selector, kubernetes_service_port_name );

    private final BiFunction<LogService,Config,RemoteMembersResolver> resolverSupplier;
    private final Collection<Setting<?>> requiredSettings;

    DiscoveryType( BiFunction<LogService,Config,RemoteMembersResolver> resolverSupplier, Setting<?>... requiredSettings )
    {
        this.resolverSupplier = resolverSupplier;
        this.requiredSettings = Arrays.asList( requiredSettings );
    }

    public RemoteMembersResolver getHostnameResolver( LogService logService, Config config )
    {
        return this.resolverSupplier.apply( logService, config );
    }

    public Collection<Setting<?>> requiredSettings()
    {
        return requiredSettings;
    }
}
