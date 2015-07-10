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
package org.neo4j.embedded;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.factory.EnterpriseEditionModule;
import org.neo4j.kernel.ha.factory.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;

/**
 * Factory methods for building a Neo4j graph database, running in an enterprise HA cluster.
 */
// TODO: when building with Java 8, the content of this class can be moved into GraphDatabase
public class EnterpriseHighAvailabilityGraphDatabase
{
    private EnterpriseHighAvailabilityGraphDatabase()
    {
    }

    /**
     * Start building a HA graph database instance with the specified cluster member identifier.
     *
     * @param memberId the id to use for this cluster member
     * @return a builder for a {@link HighAvailabilityGraphDatabase}
     */
    public static HighAvailabilityGraphDatabase.Builder withMemberId( int memberId )
    {
        return new Builder( memberId );
    }

    /**
     * A builder for a {@link HighAvailabilityGraphDatabase}
     */
    private static class Builder extends HighAvailabilityGraphDatabase.Builder
    {
        Builder( int memberId )
        {
            super( memberId );
        }

        @Override
        protected Builder self()
        {
            return this;
        }

        @Override
        protected HighAvailabilityGraphDatabase newInstance(
                File storeDir,
                LogProvider logProvider,
                Map<String,String> params,
                List<KernelExtensionFactory<?>> kernelExtensions )
        {
            GraphDatabaseFacadeFactory.Dependencies dependencies = GraphDatabaseDependencies.newDependencies()
                    .userLogProvider( logProvider )
                    .kernelExtensions( kernelExtensions )
                    .settingsClasses( GraphDatabaseSettings.class );
            EnterpriseFacadeFactory facadeFactory = new EnterpriseFacadeFactory();
            return new GraphDatabaseImpl( facadeFactory, storeDir, params, dependencies );
        }
    }

    static class GraphDatabaseImpl extends GraphDatabaseFacade implements HighAvailabilityGraphDatabase
    {
        GraphDatabaseImpl(
                EnterpriseFacadeFactory facadeFactory,
                File storeDir,
                Map<String,String> params,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            facadeFactory.newFacade( storeDir, params, dependencies, this );
        }

        @Override
        public boolean isMaster()
        {
            return ((EnterpriseEditionModule) editionModule).memberStateMachine.isMaster();
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
    }
}
