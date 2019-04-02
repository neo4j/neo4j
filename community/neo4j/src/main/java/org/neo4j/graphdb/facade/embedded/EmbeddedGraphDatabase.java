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
package org.neo4j.graphdb.facade.embedded;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.asList;

/**
 * An implementation of {@link GraphDatabaseService} that is used to embed Neo4j
 * in an application. You typically instantiate it by using
 * {@link org.neo4j.graphdb.factory.GraphDatabaseFactory} like so:
 * <p>
 *
 * <pre>
 * <code>
 * GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( &quot;var/graphdb&quot; );
 * // ... use Neo4j
 * graphDb.shutdown();
 * </code>
 * </pre>
 * <p>
 * For more information, see {@link GraphDatabaseService}.
 */
public class EmbeddedGraphDatabase extends GraphDatabaseFacade
{
    /**
     * No-op availability guard installer by default
     */
    private final AvailabilityGuardInstaller availabilityGuardInstaller;

    /**
     * Internal constructor used by {@link org.neo4j.graphdb.factory.GraphDatabaseFactory}
     */
    public EmbeddedGraphDatabase( File storeDir,
                                  Map<String, String> params,
                                  GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this.availabilityGuardInstaller = availabilityGuard -> {};
        create( storeDir, params, dependencies );
    }

    /**
     * Internal constructor used by ImpermanentGraphDatabase
     */
    protected EmbeddedGraphDatabase( File storeDir,
            Config config,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this( storeDir, config, dependencies, availabilityGuard -> {} );
    }

    protected EmbeddedGraphDatabase( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies,
            AvailabilityGuardInstaller availabilityGuardInstaller )
    {
        this.availabilityGuardInstaller = availabilityGuardInstaller;
        create( storeDir, config, dependencies );
    }

    protected void create( File storeDir, Map<String,String> params,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        GraphDatabaseDependencies newDependencies = newDependencies( dependencies )
                .settingsClasses( asList( append( GraphDatabaseSettings.class, dependencies.settingsClasses() ) ) );
        new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, this::editionModuleFactory )
                .initFacade( storeDir, params, newDependencies, this );
    }

    protected void create( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        GraphDatabaseDependencies newDependencies = newDependencies( dependencies )
                .settingsClasses( asList( append( GraphDatabaseSettings.class, dependencies.settingsClasses() ) ) );
        new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, this::editionModuleFactory )
                .initFacade( storeDir, config, newDependencies, this );
    }

    protected AbstractEditionModule editionModuleFactory( PlatformModule platform )
    {
        CommunityEditionModule edition = new CommunityEditionModule( platform );
        AvailabilityGuard guard = edition.getGlobalAvailabilityGuard( platform.clock, platform.logging, platform.config );
        availabilityGuardInstaller.install( guard );
        return edition;
    }
}
