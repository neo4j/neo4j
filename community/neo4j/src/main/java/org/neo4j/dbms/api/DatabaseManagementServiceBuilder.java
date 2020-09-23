/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.dbms.api;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.service.Services;

import static java.lang.Boolean.FALSE;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

/**
 * Creates a {@link DatabaseManagementService} with Community Edition features.
 */
@PublicApi
public class DatabaseManagementServiceBuilder
{
    protected final List<ExtensionFactory<?>> extensions = new ArrayList<>();
    protected final List<DatabaseEventListener> databaseEventListeners = new ArrayList<>();
    protected Monitors monitors;
    protected LogProvider userLogProvider = NullLogProvider.getInstance();
    protected DependencyResolver dependencies = new Dependencies();
    protected final Map<String,URLAccessRule> urlAccessRules = new HashMap<>();
    protected Path homeDirectory;
    protected Config.Builder config = Config.newBuilder();

    public DatabaseManagementServiceBuilder( Path homeDirectory )
    {
        this.homeDirectory = homeDirectory;
        Services.loadAll( ExtensionFactory.class ).forEach( extensions::add );
    }

    /**
     * @deprecated Use {@link #DatabaseManagementServiceBuilder(Path)}.
     */
    @Deprecated( forRemoval = true )
    public DatabaseManagementServiceBuilder( File homeDirectory )
    {
        this( homeDirectory.toPath() );
    }

    public DatabaseManagementService build()
    {
        config.set( GraphDatabaseSettings.neo4j_home, homeDirectory.toAbsolutePath() );
        return newDatabaseManagementService( config.build(), databaseDependencies() );
    }

    protected DatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        config.set( GraphDatabaseInternalSettings.ephemeral_lucene, FALSE );
        return new DatabaseManagementServiceFactory( getDbmsInfo( config ), getEditionFactory( config ) )
                .build( augmentConfig( config ), dependencies );
    }

    protected DbmsInfo getDbmsInfo( Config config )
    {
        return DbmsInfo.COMMUNITY;
    }

    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
    {
        return CommunityEditionModule::new;
    }

    /**
     * Override to augment config values
     * @param config
     */
    protected Config augmentConfig( Config config )
    {
        return config;
    }

    public DatabaseManagementServiceBuilder addDatabaseListener( DatabaseEventListener databaseEventListener )
    {
        databaseEventListeners.add( databaseEventListener );
        return this;
    }

    public DatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        urlAccessRules.put( protocol, rule );
        return this;
    }

    public DatabaseManagementServiceBuilder setUserLogProvider( LogProvider userLogProvider )
    {
        this.userLogProvider = userLogProvider;
        return this;
    }

    public DatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        this.monitors = monitors;
        return this;
    }

    public DatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        this.dependencies = dependencies;
        return this;
    }

    public String getEdition()
    {
        return Edition.COMMUNITY.toString();
    }

    protected ExternalDependencies databaseDependencies()
    {
        return newDependencies()
                .monitors( monitors )
                .userLogProvider( userLogProvider )
                .dependencies( dependencies )
                .urlAccessRules( urlAccessRules )
                .extensions( extensions )
                .databaseEventListeners( databaseEventListeners );
    }

    public <T> DatabaseManagementServiceBuilder setConfig( Setting<T> setting, T value )
    {
        if ( value == null )
        {
            config.remove( setting );
        }
        else
        {
            config.set( setting, value );
        }
        return this;
    }

    public DatabaseManagementServiceBuilder setConfig( Map<Setting<?>, Object> config )
    {
        this.config.set( config );
        return this;
    }

    public DatabaseManagementServiceBuilder setConfigRaw( Map<String, String> raw )
    {
        config.setRaw( raw );
        return this;
    }

    /**
     * @deprecated Use {@link #loadPropertiesFromFile(Path)} instead;
     */
    @Deprecated( forRemoval = true )
    public DatabaseManagementServiceBuilder loadPropertiesFromFile( String fileName )
    {
        return loadPropertiesFromFile( Path.of( fileName ) );
    }

    public DatabaseManagementServiceBuilder loadPropertiesFromFile( Path path )
    {
        try
        {
            config.fromFileNoThrow( path );
        }
        catch ( Exception e )
        {
            throw new IllegalArgumentException( "Unable to load " + path, e );
        }
        return this;
    }
}
