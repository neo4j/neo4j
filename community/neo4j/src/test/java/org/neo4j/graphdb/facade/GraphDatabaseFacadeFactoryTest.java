/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb.facade;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Collections;

import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.impl.factory.DatabaseInfo.COMMUNITY;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class GraphDatabaseFacadeFactoryTest
{
    @Inject
    private TestDirectory testDirectory;

    private final GraphDatabaseFacade mockFacade = mock( GraphDatabaseFacade.class );
    private final GraphDatabaseFacadeFactory.Dependencies deps = mock( GraphDatabaseFacadeFactory.Dependencies.class, RETURNS_MOCKS );

    @BeforeEach
    void setup()
    {
        when( deps.monitors() ).thenReturn( new Monitors() );
    }

    @Test
    void shouldThrowAppropriateExceptionIfStartFails()
    {
        RuntimeException startupError = new RuntimeException();
        GraphDatabaseFacadeFactory db = newFaultyGraphDatabaseFacadeFactory( startupError );
        RuntimeException startException =
                assertThrows( RuntimeException.class, () -> db.initFacade( testDirectory.storeDir(), Collections.emptyMap(), deps, mockFacade ) );
        assertEquals( startupError, Exceptions.rootCause( startException ) );
    }

    @Test
    void shouldThrowAppropriateExceptionIfBothStartAndShutdownFail()
    {
        RuntimeException startupError = new RuntimeException();
        RuntimeException shutdownError = new RuntimeException();

        GraphDatabaseFacadeFactory db = newFaultyGraphDatabaseFacadeFactory( startupError );
        doThrow( shutdownError ).when( mockFacade ).shutdown();
        RuntimeException initException =
                assertThrows( RuntimeException.class, () -> db.initFacade( testDirectory.storeDir(), Collections.emptyMap(), deps, mockFacade ) );

        assertTrue( initException.getMessage().startsWith( "Error starting " ) );
        assertEquals( startupError, initException.getCause() );
        assertEquals( shutdownError, initException.getSuppressed()[0] );
    }

    private GraphDatabaseFacadeFactory newFaultyGraphDatabaseFacadeFactory( final RuntimeException startupError )
    {
        PlatformModule platformModule = new PlatformModule( testDirectory.storeDir(), Config.defaults(), COMMUNITY, newDependencies() );
        AbstractEditionModule editionModule = new CommunityEditionModule( platformModule )
        {
            @Override
            protected SchemaWriteGuard createSchemaWriteGuard()
            {
                return SchemaWriteGuard.ALLOW_ALL_WRITES;
            }
        };
        return new GraphDatabaseFacadeFactory( DatabaseInfo.UNKNOWN, p -> editionModule )
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
            {
                final LifeSupport lifeMock = mock( LifeSupport.class );
                doThrow( startupError ).when( lifeMock ).start();
                doAnswer( invocation -> invocation.getArgument( 0 ) ).when( lifeMock ).add( any( Lifecycle.class ) );

                return new PlatformModule( storeDir, config, databaseInfo, dependencies )
                {
                    @Override
                    public LifeSupport createLife()
                    {
                        return lifeMock;
                    }
                };
            }
        };
    }
}
