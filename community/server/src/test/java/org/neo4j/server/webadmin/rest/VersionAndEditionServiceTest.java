/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.webadmin.rest;

import org.junit.Test;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.configuration.ConfigurationBuilder;
import org.neo4j.server.database.Database;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.management.VersionAndEditionService;
import org.neo4j.server.web.WebServer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VersionAndEditionServiceTest
{
    @Test
    public void shouldReturnReadableStringForServiceName() throws Exception
    {
        // given
        VersionAndEditionService service = new VersionAndEditionService( mock( CommunityNeoServer.class ) );

        // when
        String serviceName = service.getName();
        // then
        assertEquals( "version", serviceName );
    }

    @Test
    public void shouldReturnSensiblePathWhereServiceIsHosted() throws Exception
    {
        // given
        VersionAndEditionService service = new VersionAndEditionService( mock( CommunityNeoServer.class ) );

        // when
        String serverPath = service.getServerPath();

        // then
        assertEquals( "server/version", serverPath );
    }

    private AbstractNeoServer setUpMocksAndStub( Class<? extends AbstractNeoServer> serverClass )
    {
        AbstractNeoServer neoServer = mock( serverClass );
        Database database = mock( Database.class );
        final GraphDatabaseAPI graphDatabaseAPI = mock( GraphDatabaseAPI.class );
        final Version version = mock( Version.class );
        KernelData kernelData = stubKernelData( graphDatabaseAPI, version );
        when( version.getReleaseVersion() ).thenReturn( "2.0.0" );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( graphDatabaseAPI.getDependencyResolver() ).thenReturn( dependencyResolver );
        when( dependencyResolver.resolveDependency( KernelData.class ) ).thenReturn( kernelData );
        when( database.getGraph() ).thenReturn( graphDatabaseAPI );
        when( neoServer.getDatabase() ).thenReturn( database );
        return neoServer;
    }

    private KernelData stubKernelData( final GraphDatabaseAPI graphDatabaseAPI, final Version version )
    {
        return new KernelData( new Config() )
        {
            @Override
            public Version version()
            {
                return version;
            }

            @Override
            public GraphDatabaseAPI graphDatabase()
            {
                return graphDatabaseAPI;
            }
        };
    }

    private class FakeAdvancedNeoServer extends AbstractNeoServer
    {
        public FakeAdvancedNeoServer( ConfigurationBuilder configurator, Database.Factory dbFactory )
        {
            super( configurator, dbFactory, GraphDatabaseDependencies.newDependencies().logging(DevNullLoggingService.DEV_NULL ));
        }

        @Override
        protected PreFlightTasks createPreflightTasks()
        {
            throw new NotImplementedException();
        }

        @Override
        protected Iterable<ServerModule> createServerModules()
        {
            throw new NotImplementedException();
        }

        @Override
        protected WebServer createWebServer()
        {
            throw new NotImplementedException();
        }

        @Override
        public Iterable<AdvertisableService> getServices()
        {
            throw new NotImplementedException();
        }
    }

    private class FakeEnterpriseNeoServer extends FakeAdvancedNeoServer
    {
        public FakeEnterpriseNeoServer( ConfigurationBuilder configurator, Database.Factory dbFactory )
        {
            super( configurator, dbFactory );
        }
    }
}


