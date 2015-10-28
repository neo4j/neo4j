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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.embedded.TestGraphDatabase;
import org.neo4j.function.Consumer;
import org.neo4j.function.Consumers;
import org.neo4j.function.Function;
import org.neo4j.function.Functions;
import org.neo4j.function.Supplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.helpers.Pair;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.LogProvider;

public class TestGraphDatabaseRule extends ExternalResource implements Supplier<TestGraphDatabase>
{
    public static TestGraphDatabaseRule forClass( final Class<?> testClass )
    {
        return forClass( testClass, Consumers.<TestGraphDatabase.Builder>noop() );
    }

    public static TestGraphDatabaseRule forClass( final Class<?> testClass, final Consumer<TestGraphDatabase.Builder> configuration )
    {
        final TargetDirectory testDirectory = new TargetDirectory( new DefaultFileSystemAbstraction(), testClass );
        return new TestGraphDatabaseRule( new Supplier<Pair<TestGraphDatabase,Consumer<Boolean>>>()
        {
            @Override
            public Pair<TestGraphDatabase,Consumer<Boolean>> get()
            {
                final File storeDir = testDirectory.makeGraphDbDir();
                cleanup();
                TestGraphDatabase.Builder builder = TestGraphDatabase.build();
                configuration.accept( builder );
                final TestGraphDatabase graphDB = builder.open( storeDir );
                Consumer<Boolean> finalCleanup = success -> {
                    graphDB.shutdown();
                    if ( success )
                    {
                        cleanup();
                    }
                };
                return Pair.of( graphDB, finalCleanup );
            }

            private void cleanup()
            {
                try
                {
                    testDirectory.cleanup();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
    }

    public static TestGraphDatabaseRule forStoreDir( final File storeDir )
    {
        return forStoreDir( storeDir, Consumers.<TestGraphDatabase.Builder>noop() );
    }

    public static TestGraphDatabaseRule forStoreDir( final File storeDir, final Consumer<TestGraphDatabase.Builder> configuration )
    {
        return new TestGraphDatabaseRule( new Supplier<Pair<TestGraphDatabase,Consumer<Boolean>>>()
        {
            @Override
            public Pair<TestGraphDatabase,Consumer<Boolean>> get()
            {
                TestGraphDatabase.Builder builder = TestGraphDatabase.build();
                configuration.accept( builder );
                final TestGraphDatabase graphDB = builder.open( storeDir );
                Consumer<Boolean> finalCleanup = success -> {
                    graphDB.shutdown();
                    if ( success )
                    {
                        cleanup();
                    }
                };
                return Pair.of( graphDB, finalCleanup );
            }

            private void cleanup()
            {
                try
                {
                    FileUtils.deleteRecursively( storeDir );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
    }

    public static TestGraphDatabaseRule ephemeral()
    {
        return ephemeral( Consumers.<TestGraphDatabase.EphemeralBuilder>noop() );
    }

    public static TestGraphDatabaseRule ephemeral( final LogProvider logProvider )
    {
        return ephemeral( builder -> {
            builder.withLogProvider( logProvider ).withInternalLogProvider( logProvider );
        } );
    }

    public static TestGraphDatabaseRule ephemeral( final Consumer<TestGraphDatabase.EphemeralBuilder> configuration )
    {
        return new TestGraphDatabaseRule( () -> {
            final EphemeralFileSystemAbstraction fs = new EphemeralFileSystemAbstraction();
            TestGraphDatabase.EphemeralBuilder builder = TestGraphDatabase.buildEphemeral().withFileSystem( fs );
            configuration.accept( builder );
            final TestGraphDatabase graphDatabase = builder.open();
            Consumer<Boolean> cleanup1 = success -> {
                graphDatabase.shutdown();
                fs.shutdown();
            };
            return Pair.of( graphDatabase, cleanup1 );
        } );
    }


    private final Supplier<Pair<TestGraphDatabase,Consumer<Boolean>>> resourceSupplier;

    private TestGraphDatabase graphDb;
    private Consumer<Boolean> cleanup;

    protected TestGraphDatabaseRule( Supplier<Pair<TestGraphDatabase,Consumer<Boolean>>> resourceSupplier )
    {
        this.resourceSupplier = resourceSupplier;
    }

    @Override
    public TestGraphDatabase get()
    {
        if ( graphDb == null )
        {
            synchronized ( this )
            {
                if ( graphDb == null )
                {
                    Pair<TestGraphDatabase,Consumer<Boolean>> pair = resourceSupplier.get();
                    graphDb = pair.first();
                    cleanup = pair.other();
                }
            }
        }
        return graphDb;
    }

    @Override
    protected void after( boolean successful ) throws Throwable
    {
        if ( cleanup != null )
        {
            cleanup.accept( successful );
        }
    }

    public <T> T when( Function<GraphDatabaseService,T> function )
    {
        return function.apply( get() );
    }

    public Transaction beginTx()
    {
        return get().beginTx();
    }

    public Result execute( String query ) throws QueryExecutionException
    {
        return get().execute( query );
    }

    public Result execute( String query, Map<String,Object> parameters ) throws QueryExecutionException
    {
        return get().execute( query, parameters );
    }

    public void executeAndCommit( Consumer<? super GraphDatabaseService> consumer )
    {
        executeAndCommit( Functions.fromConsumer( consumer ) );
    }

    public <T> T executeAndCommit( Function<? super GraphDatabaseService,T> function )
    {
        GraphDatabaseService gds = get();
        try ( Transaction tx = gds.beginTx() )
        {
            T result = function.apply( gds );

            tx.success();
            return result;
        }
    }

    public <T> T executeAndRollback( Function<? super GraphDatabaseService,T> function )
    {
        GraphDatabaseService gds = get();
        try ( Transaction tx = gds.beginTx() )
        {
            return function.apply( gds );
        }
    }

    public <FROM, TO> AlgebraicFunction<FROM,TO> tx( final Function<FROM,TO> function )
    {
        return new AlgebraicFunction<FROM,TO>()
        {
            @Override
            public TO apply( final FROM from )
            {
                return executeAndCommit( graphDb -> {
                    return function.apply( from );
                } );
            }
        };
    }

    public Node createNode( Label... labels )
    {
        return get().createNode( labels );
    }

    public Schema schema()
    {
        return get().schema();
    }

    public IndexManager index()
    {
        return get().index();
    }

    public <T> T resolveDependency( Class<T> type )
    {
        return get().getDependencyResolver().resolveDependency( type );
    }
}
