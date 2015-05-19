/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.desktop.runtime;

import java.net.BindException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.MainWindow;
import org.neo4j.desktop.ui.UnableToStartServerException;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.ServerStartupException;
import org.neo4j.server.configuration.ConfigurationBuilder;
import static org.neo4j.kernel.logging.DefaultLogging.createDefaultLogging;

/**
 * Lifecycle actions for the Neo4j server living inside this JVM. Typically reacts to button presses
 * from {@link MainWindow}.
 */
public class DatabaseActions
{
    private final DesktopModel model;
    private AbstractNeoServer server;
    private Logging logging;
    private LifeSupport life;

    public DatabaseActions( DesktopModel model )
    {
        this.model = model;
    }

    public void start() throws UnableToStartServerException
    {
        if ( isRunning() )
        {
            throw new UnableToStartServerException( "Already started" );
        }
        life = new LifeSupport();

        ConfigurationBuilder configurator = model.getServerConfigurator();
        Monitors monitors = new Monitors();
        logging = life.add( createDefaultLogging( configurator.getDatabaseTuningProperties(), monitors ) );
        life.start();
        server = new CommunityNeoServer( configurator, GraphDatabaseDependencies.newDependencies().logging(logging).monitors( monitors ) );
        try
        {
            server.start();
        }
        catch ( ServerStartupException e )
        {
            server = null;
            Set<Class> causes = extractCauseTypes( e );
            if ( causes.contains( StoreLockException.class ) )
            {
                throw new UnableToStartServerException(
                        "Unable to lock store. Are you running another Neo4j process against this database?" );
            }
            if ( causes.contains( BindException.class ) )
            {
                throw new UnableToStartServerException(
                        "Unable to bind to port. Are you running another Neo4j process on this computer?" );
            }
            throw new UnableToStartServerException( e.getMessage() );
        }
    }

    private Set<Class> extractCauseTypes( Throwable e )
    {
        Set<Class> types = new HashSet<>();
        types.add( e.getClass() );
        if ( e.getCause() != null )
        {
            types.addAll( extractCauseTypes( e.getCause() ) );
        }
        return types;
    }

    public void stop()
    {
        if ( isRunning() )
        {
            server.stop();
            server = null;
            life.shutdown();
        }
    }

    public void shutdown()
    {
        if ( isRunning() )
        {
            stop();
        }
    }

    public boolean isRunning()
    {
        return server != null;
    }
}
