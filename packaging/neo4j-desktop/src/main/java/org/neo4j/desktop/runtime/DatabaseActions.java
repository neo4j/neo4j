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

import java.io.File;
import java.util.List;

import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.ui.MainWindow;
import org.neo4j.server.Bootstrapper;

/**
 * Lifecycle actions for the Neo4j server living inside this JVM. Typically reacts to button presses
 * from {@link MainWindow}.
 */
public class DatabaseActions
{
    private Bootstrapper server;
    private final Value<List<String>> extensionPackages;
    
    public DatabaseActions( Value<List<String>> extensionPackages )
    {
        this.extensionPackages = extensionPackages;
    }

    public void start( String path )
    {
        if ( isRunning() )
        {
            throw new IllegalStateException( "Already started" );
        }

        server = new DesktopBootstrapper( new File( path ),
                getDatabaseConfigurationFile( path ), extensionPackages );
        server.start();
    }
    
    public File getDatabaseConfigurationFile( String path )
    {
        return new File( path, "neo4j.properties" );
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
