/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.database;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DatabaseRegistryTest
{
    private static final String EMBEDDED = "embedded";
    private final Database northwind = mock(Database.class);

    @Rule
    public OtherThreadRule<Object> threadOne = new OtherThreadRule<>();

    @Rule
    public OtherThreadRule<Object> threadTwo = new OtherThreadRule<>();

    @Test
    public void shouldAllowCreatingAndVisitingDatabase() throws Throwable
    {
        // Given
        DatabaseRegistry registry = newRegistryWithEmbeddedProvider();
        final AtomicReference<Database> dbProvidedToVisitor = new AtomicReference<>(  );
        registry.create( new DatabaseDefinition( "northwind", EMBEDDED, DatabaseHosting.Mode.EXTERNAL, new Config() ));

        // When
        registry.visit( "northwind", new DatabaseRegistry.Visitor()
        {
            @Override
            public void visit( Database db )
            {
                dbProvidedToVisitor.set( db );
            }
        } );

        // Then
        assertThat(dbProvidedToVisitor.get(), equalTo(northwind));
        verify( northwind ).init();
        verify( northwind ).start();
    }

    @Test
    public void shouldShutdownDatabaseOnDrop() throws Throwable
    {
        // Given
        DatabaseRegistry registry = newRegistryWithEmbeddedProvider();
        registry.create( new DatabaseDefinition( "northwind", EMBEDDED, DatabaseHosting.Mode.EXTERNAL, new Config()) );

        // When
        registry.drop( "northwind" );

        // Then
        verify( northwind ).init();
        verify( northwind ).start();
        verify( northwind ).stop();
        verify( northwind ).shutdown();
    }

    @Ignore("Saw this fail in the assertion at line 109. This component is unused and JH will remove it")
    @Test
    public void shouldAwaitRunningQueriesBeforeDropping() throws Throwable
    {
        // Given
        DatabaseRegistry registry = newRegistryWithEmbeddedProvider();
        registry.create( new DatabaseDefinition("northwind", EMBEDDED, DatabaseHosting.Mode.EXTERNAL, new Config()) );

        CountDownLatch visitingDbLatch = new CountDownLatch( 1 );
        threadOne.execute( visitAndAwaitLatch( "northwind", registry, visitingDbLatch ) );

        // When
        Future<Object> threadTwoCompletion = threadTwo.execute( drop( "northwind", registry ) );

        // Then, even if I wait a while, the database should not be shut down
        Thread.sleep( 100 );
        verify( northwind, never() ).stop();
        verify( northwind, never() ).shutdown();

        // But when
        visitingDbLatch.countDown();
        threadTwoCompletion.get( 10, TimeUnit.SECONDS );

        // Then
        verify(northwind).stop();
        verify(northwind).shutdown();
    }

    private OtherThreadExecutor.WorkerCommand<Object, Object> drop( final String dbKey, final DatabaseRegistry registry )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Object>()
        {
            @Override
            public Object doWork( Object state ) throws Exception
            {
                registry.drop( dbKey );
                return null;
            }
        };
    }

    private OtherThreadExecutor.WorkerCommand<Object, Object> visitAndAwaitLatch( final String dbKey,
                                                                                  final DatabaseRegistry registry,
                                                                                  final CountDownLatch latchToAwait )
    {
        return new OtherThreadExecutor.WorkerCommand<Object, Object>()
        {
            @Override
            public Object doWork( Object state ) throws Exception
            {
                registry.visit( dbKey, new DatabaseRegistry.Visitor()
                {
                    @Override
                    public void visit( Database db )
                    {
                        try
                        {
                            latchToAwait.await( 10, TimeUnit.SECONDS );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                } );
                return null;
            }
        };
    }

    private DatabaseRegistry newRegistryWithEmbeddedProvider()
    {
        DatabaseRegistry registry = new DatabaseRegistry( Functions.<Config, InternalAbstractGraphDatabase.Dependencies>constant(GraphDatabaseDependencies.newDependencies().logging(new TestLogging() ) ));
        registry.addProvider( EMBEDDED, singletonDatabase( northwind ) );
        registry.init();
        registry.start();
        return registry;
    }

    public static Database.Factory singletonDatabase( final Database db )
    {
        return new Database.Factory()
        {
            @Override
            public Database newDatabase(Config config, InternalAbstractGraphDatabase.Dependencies dependencies)
            {
                return db;
            }
        };
    }
}
