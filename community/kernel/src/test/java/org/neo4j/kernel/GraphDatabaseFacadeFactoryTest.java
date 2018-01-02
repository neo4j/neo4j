/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.test.TargetDirectory;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class GraphDatabaseFacadeFactoryTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir =
            TargetDirectory.testDirForTestWithEphemeralFS( new EphemeralFileSystemAbstraction(), getClass() );

    private final GraphDatabaseFacade mockFacade = mock( GraphDatabaseFacade.class );
    private final GraphDatabaseFacadeFactory.Dependencies deps =
            mock( GraphDatabaseFacadeFactory.Dependencies.class, RETURNS_MOCKS );

    @Test
    public void shouldThrowAppropriateExceptionIfStartFails()
    {
        // Given
        RuntimeException startupError = new RuntimeException();

        GraphDatabaseFacadeFactory db = newFaultyGraphDatabaseFacadeFactory( startupError );
        try
        {
            // When
            db.newFacade( dir.graphDbDir(), Collections.<String,String>emptyMap(), deps, mockFacade,
                    OperationalMode.single );
            fail( "Should have thrown " + RuntimeException.class );
        }
        catch ( RuntimeException exception )
        {
            // Then
            assertEquals( startupError, Exceptions.rootCause( exception ) );
        }
    }

    @Test
    public void shouldThrowAppropriateExceptionIfBothStartAndShutdownFail()
    {
        // Given
        RuntimeException startupError = new RuntimeException();
        RuntimeException shutdownError = new RuntimeException();

        GraphDatabaseFacadeFactory db = newFaultyGraphDatabaseFacadeFactory( startupError );
        doThrow( shutdownError ).when( mockFacade ).shutdown();
        try
        {
            // When
            db.newFacade( dir.graphDbDir(), Collections.<String,String>emptyMap(), deps, mockFacade,
                    OperationalMode.single );
            fail( "Should have thrown " + RuntimeException.class );
        }
        catch ( RuntimeException exception )
        {
            // Then
            assertEquals( shutdownError, exception );
            assertEquals( startupError, Exceptions.rootCause( exception.getSuppressed()[0] ) );
        }
    }

    private GraphDatabaseFacadeFactory newFaultyGraphDatabaseFacadeFactory( final RuntimeException startupError )
    {
        return new GraphDatabaseFacadeFactory()
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Map<String,String> params,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                    OperationalMode operationalMode )
            {
                final LifeSupport lifeMock = mock( LifeSupport.class );
                doThrow( startupError ).when( lifeMock ).start();
                doAnswer( new Answer()
                {
                    @Override
                    public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
                    {
                        return invocationOnMock.getArguments()[0];
                    }
                } ).when( lifeMock ).add( any( Lifecycle.class ) );


                return new PlatformModule( storeDir, params, dependencies, graphDatabaseFacade, operationalMode )
                {
                    @Override
                    public LifeSupport createLife()
                    {
                        return lifeMock;
                    }
                };
            }

            @Override
            protected EditionModule createEdition( PlatformModule platformModule )
            {
                return null;
            }

            @Override
            protected DataSourceModule createDataSource(
                    Dependencies dependencies, PlatformModule platformModule, EditionModule editionModule )
            {
                return null;
            }
        };
    }
}
