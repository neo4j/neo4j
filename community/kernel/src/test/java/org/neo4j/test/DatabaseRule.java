/**
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

import org.junit.rules.ExternalResource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public abstract class DatabaseRule extends ExternalResource
{
    GraphDatabaseBuilder databaseBuilder;
    GraphDatabaseAPI database;
    private String storeDir;

    public <T> T when( Function<GraphDatabaseService, T> function )
    {
        return function.apply( getGraphDatabaseService() );
    }

    public <T> T executeAndCommit( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, true );
    }

    public <T> T executeAndRollback( Function<? super GraphDatabaseService, T> function )
    {
        return transaction( function, false );
    }

    public <FROM, TO> AlgebraicFunction<FROM, TO> tx( final Function<FROM, TO> function )
    {
        return new AlgebraicFunction<FROM, TO>()
        {
            @Override
            public TO apply( final FROM from )
            {
                return executeAndCommit( new Function<GraphDatabaseService, TO>()
                {
                    @Override
                    public TO apply( GraphDatabaseService graphDb )
                    {
                        return function.apply( from );
                    }
                } );
            }
        };
    }

    private <T> T transaction( Function<? super GraphDatabaseService, T> function, boolean commit )
    {
        try ( Transaction tx = database.beginTx() )
        {
            T result = function.apply( database );

            if ( commit )
            {
                tx.success();
            }
            return result;
        }
    }

    @Override
    protected void before() throws Throwable
    {
        create();
    }

    @Override
    protected void after()
    {
        shutdown();
    }

    @SuppressWarnings("deprecation")
    public void create() throws IOException
    {
        createResources();
        try
        {
            GraphDatabaseFactory factory = newFactory();
            configure( factory );
            databaseBuilder = newBuilder( factory );
            configure( databaseBuilder );
            database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
            storeDir = database.getStoreDir();
        }
        catch ( RuntimeException e )
        {
            deleteResources();
            throw e;
        }
    }

    protected void deleteResources()
    {
    }

    protected void createResources() throws IOException
    {
    }

    protected abstract GraphDatabaseFactory newFactory();

    protected abstract GraphDatabaseBuilder newBuilder( GraphDatabaseFactory factory );

    protected void configure( GraphDatabaseFactory databaseFactory )
    {
        // Override to configure the database factory
    }

    protected void configure( GraphDatabaseBuilder builder )
    {
        // Override to configure the database
    }

    public GraphDatabaseService getGraphDatabaseService()
    {
        return database;
    }

    public GraphDatabaseAPI getGraphDatabaseAPI()
    {
        return database;
    }
    
    public static interface RestartAction
    {
        void run( FileSystemAbstraction fs, File storeDirectory );
    }

    public void restartDatabase( RestartAction action )
    {
        FileSystemAbstraction fs = database.getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
        database.shutdown();
        action.run( fs, new File( storeDir ) );
        database = (GraphDatabaseAPI) databaseBuilder.newGraphDatabase();
    }

    public void shutdown()
    {
        try
        {
            if ( database != null )
            {
                database.shutdown();
            }
        }
        finally
        {
            deleteResources();
            database = null;
        }
    }

    public void clearCache()
    {
        getGraphDatabaseAPI().getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
    }
}
