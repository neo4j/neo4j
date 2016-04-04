/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.builder.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.builder.GraphDatabaseFacadeFactorySelector;
import org.neo4j.graphdb.factory.builder.PriorityFacadeFactorySelector;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

/**
 * Creates a {@link org.neo4j.graphdb.GraphDatabaseService}.
 * <p>
 * Use {@link #newEmbeddedDatabase(File)} or
 * {@link #newEmbeddedDatabaseBuilder(File)} to create a database instance.
 */
public class GraphDatabaseFactory
{
    private final GraphDatabaseFactoryState state;

    public GraphDatabaseFactory()
    {
        this( new GraphDatabaseFactoryState() );
    }

    protected GraphDatabaseFactory( GraphDatabaseFactoryState state )
    {
        this.state = state;
    }

    protected GraphDatabaseFactoryState getCurrentState()
    {
        return state;
    }

    protected GraphDatabaseFactoryState getStateCopy()
    {
        return new GraphDatabaseFactoryState( getCurrentState() );
    }

    public GraphDatabaseService newEmbeddedDatabase( File storeDir )
    {
        return newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
    }

    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( File storeDir, Edition edition )
    {
        final GraphDatabaseFactoryState state = getStateCopy();
        if (edition != null)
        {
            state.setEdition( edition );
        }
        GraphDatabaseBuilder.DatabaseCreator creator = createDatabaseCreator( storeDir, state );
        GraphDatabaseBuilder builder = createGraphDatabaseBuilder( creator );
        configure( builder );
        return builder;
    }

    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( File storeDir )
    {
        return newEmbeddedDatabaseBuilder( storeDir, null );
    }

    protected GraphDatabaseBuilder createGraphDatabaseBuilder( GraphDatabaseBuilder.DatabaseCreator creator )
    {
        return new GraphDatabaseBuilder( creator );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( final File storeDir,
            final GraphDatabaseFactoryState state )
    {
        return this.createDatabaseCreator( storeDir, state, selectDatabaseFacadeFactory( state.getFactorySelector() ) );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( final File storeDir,
            final GraphDatabaseFactoryState state, GraphDatabaseFacadeFactory facadeFactory )
    {
        return new DefaultDatabaseCreator( storeDir, facadeFactory );
    }

    private GraphDatabaseFacadeFactory selectDatabaseFacadeFactory(GraphDatabaseFacadeFactorySelector selector)
    {
        Iterable<GraphDatabaseFacadeFactory> databaseFacadeFactories = Service.load( GraphDatabaseFacadeFactory.class );
        List<GraphDatabaseFacadeFactory> facadeFactories = Iterables.asList( databaseFacadeFactories );
        return selector.select( facadeFactories );
    }

    protected void configure( GraphDatabaseBuilder builder )
    {
        // Let the default configuration pass through.
    }

    protected GraphDatabaseService newDatabase( File storeDir, Map<String,String> config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return selectDatabaseFacadeFactory( new PriorityFacadeFactorySelector() ).newFacade( storeDir, config, dependencies );
    }

    public GraphDatabaseFactory addURLAccessRule( String protocol, URLAccessRule rule )
    {
        getCurrentState().addURLAccessRule( protocol, rule );
        return this;
    }

    public GraphDatabaseFactory setUserLogProvider( LogProvider userLogProvider )
    {
        getCurrentState().setUserLogProvider( userLogProvider );
        return this;
    }

    public GraphDatabaseFactory setMonitors( Monitors monitors )
    {
        getCurrentState().setMonitors( monitors );
        return this;
    }

    // TODO: more effective way should be introduced
    public String getEdition()
    {
        return selectDatabaseFacadeFactory( new PriorityFacadeFactorySelector() ).databaseInfo().edition.toString();
    }

    public class DefaultDatabaseCreator implements GraphDatabaseBuilder.DatabaseCreator
    {
        private File storeDirectory;
        private GraphDatabaseFacadeFactory facadeFactory;

        public DefaultDatabaseCreator( File storeDirectory, GraphDatabaseFacadeFactory facadeFactory )
        {
            this.storeDirectory = storeDirectory;
            this.facadeFactory = facadeFactory;
        }

        @Override
        public GraphDatabaseService newDatabase( Map<String,String> config )
        {
            config.put( "unsupported.dbms.ephemeral", "false" );
            GraphDatabaseFacadeFactory.Dependencies dependencies = state.databaseDependencies();
            return facadeFactory.newFacade( storeDirectory, config, dependencies );
        }
    }
}
