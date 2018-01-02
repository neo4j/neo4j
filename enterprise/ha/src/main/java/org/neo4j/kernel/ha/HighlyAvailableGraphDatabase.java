/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.factory.HighlyAvailableEditionModule;
import org.neo4j.kernel.ha.factory.HighlyAvailableFacadeFactory;
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
 */
public class HighlyAvailableGraphDatabase extends GraphDatabaseFacade
{
    private HighlyAvailableEditionModule highlyAvailableEditionModule;

    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions,
                                         Monitors monitors )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ).monitors( monitors ) );
    }

    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params,
                                         Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class, ClusterSettings.class, HaSettings.class )
                .kernelExtensions( kernelExtensions ) );
    }

    public HighlyAvailableGraphDatabase( File storeDir, Map<String,String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        newHighlyAvailableFacadeFactory().newFacade( storeDir, params, dependencies, this );
    }

    protected HighlyAvailableFacadeFactory newHighlyAvailableFacadeFactory()
    {
        return new HighlyAvailableFacadeFactory();
    }

    @Override
    public void init( PlatformModule platformModule, EditionModule editionModule, DataSourceModule dataSourceModule )
    {
        super.init( platformModule, editionModule, dataSourceModule );
        this.highlyAvailableEditionModule = (HighlyAvailableEditionModule) editionModule;
    }

    public HighAvailabilityMemberState getInstanceState()
    {
        return highlyAvailableEditionModule.memberStateMachine.getCurrentState();
    }

    public String role()
    {
        return highlyAvailableEditionModule.members.getCurrentMemberRole();
    }

    public boolean isMaster()
    {
        return HighAvailabilityModeSwitcher.MASTER.equals( role() );
    }

    public File getStoreDirectory()
    {
        return storeDir;
    }
}
