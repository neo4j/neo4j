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
package org.neo4j.graphdb.factory;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.logging.LogProvider;

import static java.util.Arrays.asList;

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

    /**
     * @deprecated use {@link #newEmbeddedDatabase(File)} instead.
     * @param storeDir the location of the database
     * @return the database
     */
    @Deprecated
    public GraphDatabaseService newEmbeddedDatabase( String storeDir )
    {
        return newEmbeddedDatabase( new File( storeDir ) );
    }

    public GraphDatabaseService newEmbeddedDatabase( File storeDir )
    {
        return newEmbeddedDatabaseBuilder( storeDir ).newGraphDatabase();
    }

    /**
     * @deprecated use {@link #newEmbeddedDatabaseBuilder(File)} instead
     * @param storeDir the location of the database
     * @return a builder which is used to configure and start a database
     */
    @Deprecated
    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( String storeDir )
    {
        return newEmbeddedDatabaseBuilder( new File( storeDir ) );
    }

    public GraphDatabaseBuilder newEmbeddedDatabaseBuilder( File storeDir )
    {
        final GraphDatabaseFactoryState state = getStateCopy();
        GraphDatabaseBuilder.DatabaseCreator creator = createDatabaseCreator( storeDir, state );
        GraphDatabaseBuilder builder = createGraphDatabaseBuilder( creator );
        configure( builder );
        return builder;
    }

    protected GraphDatabaseBuilder createGraphDatabaseBuilder( GraphDatabaseBuilder.DatabaseCreator creator )
    {
        return new GraphDatabaseBuilder( creator );
    }

    protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator(
            final File storeDir, final GraphDatabaseFactoryState state )
    {
        return new GraphDatabaseBuilder.DatabaseCreator()
        {
            @Override
            public GraphDatabaseService newDatabase( Map<String,String> config )
            {
                config.put( "ephemeral", "false" );
                GraphDatabaseFacadeFactory.Dependencies dependencies = state.databaseDependencies();
                return GraphDatabaseFactory.this.newDatabase( storeDir, config, dependencies );
            }
        };
    }

    protected void configure( GraphDatabaseBuilder builder )
    {
        // Let the default configuration pass through.
    }

    protected GraphDatabaseService newDatabase( File storeDir, Map<String,String> config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        return new CommunityFacadeFactory().newFacade( storeDir, config, dependencies );
    }

    /**
     * @deprecated Manipulating kernel extensions is deprecated and will be moved to internal components.
     * 
     * @return kernel extensions
     */
    @Deprecated
    public Iterable<KernelExtensionFactory<?>> getKernelExtension()
    {
        return getCurrentState().getKernelExtension();
    }

    /**
     * @deprecated Manipulating kernel extensions is deprecated and will be moved to internal components.
     * 
     * @param newKernelExtensions New kernel extensions to add
     * @return the factory
     */
    @Deprecated
    public GraphDatabaseFactory addKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().addKernelExtensions( newKernelExtensions );
        return this;
    }

    /**
     * @deprecated Manipulating kernel extensions is deprecated and will be moved to internal components.
     * 
     * @param newKernelExtension New kernel extension too add
     * @return the factory
     */
    @Deprecated
    @SuppressWarnings({"rawtypes", "unchecked"})
    public GraphDatabaseFactory addKernelExtension( KernelExtensionFactory<?> newKernelExtension )
    {
        List extensions = asList( newKernelExtension );
        return addKernelExtensions( extensions );
    }

    /**
     * @deprecated Manipulating kernel extensions is deprecated and will be moved to internal components.
     * 
     * @param newKernelExtensions New kernel extensions to set
     * @return the factory
     */
    @Deprecated
    public GraphDatabaseFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
    {
        getCurrentState().setKernelExtensions( newKernelExtensions );
        return this;
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

    public String getEdition()
    {
        return "Community";
    }
}
