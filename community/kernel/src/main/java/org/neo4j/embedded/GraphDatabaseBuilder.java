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
package org.neo4j.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

/**
 * A builder for a GraphDatabase.
 *
 * @param <BUILDER> The concrete type of the builder
 * @param <GRAPHDB> The concrete type of the GraphDatabase being built
 */
abstract class GraphDatabaseBuilder<BUILDER extends GraphDatabaseBuilder<BUILDER,GRAPHDB>, GRAPHDB extends GraphDatabase>
{
    private LogProvider logProvider;
    protected final Map<String,String> params = new HashMap<>();
    protected List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();

    protected GraphDatabaseBuilder()
    {
        withLogProvider( NullLogProvider.getInstance() );
        params.put( "ephemeral", "false" );
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
    }

    protected abstract BUILDER self();

    protected abstract GRAPHDB newInstance(
            File storeDir,
            LogProvider logProvider,
            Map<String,String> params,
            List<KernelExtensionFactory<?>> kernelExtensions );

    /**
     * Set an internal parameter. Use of this method is not recommended.
     *
     * @param key the parameter key
     * @param value the parameter value
     * @return this builder
     */
    public BUILDER withParam( String key, String value )
    {
        this.params.put( key, value );
        return self();
    }

    /**
     * Set internal parameters. Use of this method is not recommended.
     *
     * @param params parameters to add
     * @return this builder
     */
    public BUILDER withParams( Map<String,String> params )
    {
        this.params.putAll( params );
        return self();
    }

    /**
     * Configure a database setting.
     *
     * @param setting the setting instance to be set
     * @param value the setting value
     * @return this builder
     */
    public BUILDER withSetting( Setting setting, String value )
    {
        this.params.put( setting.name(), value );
        return self();
    }

    /**
     * Specify a {@link LogProvider} that will be used for logging within the graph database.
     *
     * @param logProvider a {@link LogProvider} to use for logging
     * @return this builder
     */
    public BUILDER withLogProvider( LogProvider logProvider )
    {
        this.logProvider = logProvider;
        return self();
    }

    /**
     * Allow this graph database instance to upgrade the store if required.
     *
     * @return this builder
     */
    public BUILDER allowStoreUpgrade()
    {
        params.put( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
        return self();
    }

    /**
     * Open this graph database in read-only mode, allowing only read operations.
     *
     * @return this builder
     */
    public BUILDER readOnly()
    {
        params.put( GraphDatabaseSettings.read_only.name(), "true" );
        return self();
    }

    /**
     * Starts the graph database.
     *
     * @param storeDir The filesystem location for the store, which will be created if necessary
     * @return The running database
     */
    public GRAPHDB open( File storeDir )
    {
        return newInstance( storeDir, logProvider, params, kernelExtensions );
    }
}
