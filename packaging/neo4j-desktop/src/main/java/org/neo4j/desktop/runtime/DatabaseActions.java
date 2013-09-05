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
import org.neo4j.server.Bootstrapper;

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

    public void start()
    {
        if ( isRunning() )
        {
            throw new IllegalStateException( "Already started" );
        }

        server = new DesktopBootstrapper(
                model.getDatabaseDirectory(),
                model.getDatabaseConfigurationFile(),
                new ArrayList<String>() );
        server.start();
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
