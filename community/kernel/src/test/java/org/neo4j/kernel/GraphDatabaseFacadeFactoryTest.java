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
package org.neo4j.kernel;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.mockito.Mockito;

import java.io.File;
import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DataSourceModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GraphDatabaseFacadeFactoryTest
{

    private final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory( fileSystemRule.get() );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( dir ).around( fileSystemRule );

    private final GraphDatabaseFacade mockFacade = mock( GraphDatabaseFacade.class );
    private final GraphDatabaseFacadeFactory.Dependencies deps =
            mock( GraphDatabaseFacadeFactory.Dependencies.class, RETURNS_MOCKS );

    @Before
    public void setup()
    {
        when( deps.monitors() ).thenReturn( new Monitors() );
    }

    @Test
    public void shouldThrowAppropriateExceptionIfStartFails()
    {
        // Given
        RuntimeException startupError = new RuntimeException();

        GraphDatabaseFacadeFactory db = newFaultyGraphDatabaseFacadeFactory( startupError );
        try
        {
            // When
            db.initFacade( dir.graphDbDir(), Collections.emptyMap(), deps, mockFacade );
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
            db.initFacade( dir.graphDbDir(), Collections.emptyMap(), deps, mockFacade );
            fail( "Should have thrown " + RuntimeException.class );
        }
        catch ( RuntimeException exception )
        {
            // Then
            assertTrue( exception.getMessage().startsWith( "Error starting " ) );
            assertEquals( startupError, exception.getCause() );
            assertEquals( shutdownError, exception.getSuppressed()[0] );
        }
    }

    private GraphDatabaseFacadeFactory newFaultyGraphDatabaseFacadeFactory( final RuntimeException startupError )
    {
        return new GraphDatabaseFacadeFactory( DatabaseInfo.UNKNOWN,
                p -> mock( EditionModule.class, Mockito.RETURNS_DEEP_STUBS ) )
        {
            @Override
            protected PlatformModule createPlatform( File storeDir, Config config,
                    Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade )
            {
                final LifeSupport lifeMock = mock( LifeSupport.class );
                doThrow( startupError ).when( lifeMock ).start();
                doAnswer( invocation -> invocation.getArgument( 0 ) ).when( lifeMock ).add( any( Lifecycle.class ) );

                return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
                {
                    @Override
                    public LifeSupport createLife()
                    {
                        return lifeMock;
                    }
                };
            }

            @Override
            protected DataSourceModule createDataSource( PlatformModule platformModule, EditionModule editionModule,
                    Supplier<QueryExecutionEngine> queryExecutionEngineSupplier )
            {
                return mock( DataSourceModule.class );
            }
        };
    }
}
