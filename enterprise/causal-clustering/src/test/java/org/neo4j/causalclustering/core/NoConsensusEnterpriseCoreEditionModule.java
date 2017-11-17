/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class NoConsensusEnterpriseCoreEditionModule extends EnterpriseCoreEditionModule
{

    NoConsensusEnterpriseCoreEditionModule( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory )
    {
        super( platformModule, discoveryServiceFactory, NoOpConsensusModule::new );
    }

    /**
     * TODO This method must be moved to another package
     *
     * @param platformModule
     * @param discoveryServiceFactory
     * @return
     */
    public static NoConsensusEnterpriseCoreEditionModule refactorThisHack( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        return new NoConsensusEnterpriseCoreEditionModule( platformModule, discoveryServiceFactory );
    }
}
