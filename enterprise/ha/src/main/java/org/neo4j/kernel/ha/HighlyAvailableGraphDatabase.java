/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.io.File;
import java.util.Map;

import org.neo4j.embedded.HighAvailabilityGraphDatabase;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.factory.EnterpriseEditionModule;
import org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

/**
 * This has all the functionality of an embedded database, with the addition of services
 * for handling clustering.
 *
 * @deprecated use {@link HighAvailabilityGraphDatabase} instead
 */
@Deprecated
public class HighlyAvailableGraphDatabase extends GraphDatabaseFacade implements HighAvailabilityGraphDatabase
{
    private EnterpriseEditionModule enterpriseEditionModule;

    @Deprecated
    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Monitors monitors )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ).monitors( monitors ) );
    }

    @Deprecated
    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ) );
    }

    @Deprecated
    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new EnterpriseFacadeFactory().newFacade( storeDir, params, dependencies, this );
    }

    @Override
    public void init( PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule )
    {
        super.init( platformModule, editionModule, dataSourceModule );
        this.enterpriseEditionModule = (EnterpriseEditionModule) editionModule;
    }

    public HighAvailabilityMemberState getInstanceState()
    {
        return enterpriseEditionModule.memberStateMachine.getCurrentState();
    }

    public String role()
    {
        return enterpriseEditionModule.members.getSelf().getHARole();
    }

    @Override
    public Role haClusterRole()
    {
        ClusterMember self = ((EnterpriseEditionModule) editionModule).members.getSelf();
        if ( self.hasRole( HighAvailabilityModeSwitcher.MASTER ) )
        {
            return Role.MASTER;
        }
        else if ( self.hasRole( HighAvailabilityModeSwitcher.SLAVE ) )
        {
            return Role.SLAVE;
        }
        else
        {
            return Role.UNKNOWN;
        }
    }

    public boolean isMaster()
    {
        return enterpriseEditionModule.memberStateMachine.isMaster();
    }

    public File getStoreDirectory()
    {
        return storeDir;
    }
}
