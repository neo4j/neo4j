/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;

import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.MainWindow;
import org.neo4j.desktop.ui.UnableToStartServerException;
import org.neo4j.server.Bootstrapper;

import static org.neo4j.server.Bootstrapper.OK;

/**
 * Lifecycle actions for the Neo4j server living inside this JVM. Typically reacts to button presses
 * from {@link MainWindow}.
 */
public class DatabaseActions
{
    private final DesktopModel model;
    private Bootstrapper server;

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

        server = new DesktopBootstrapper(
                model.getDatabaseDirectory(),
                model.getDatabaseConfigurationFile(),
                new ArrayList<String>() );

        int startupCode = server.start();

        if (startupCode == OK) return;

        stop();

        throw new UnableToStartServerException( "Unable to start Neo4j Server on port 7474. See logs for details." );
    }
    
    public void stop()
    {
        if ( !isRunning() )
        {
            throw new IllegalStateException( "Not started" );
        }

        server.stop();
        server = null;
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
