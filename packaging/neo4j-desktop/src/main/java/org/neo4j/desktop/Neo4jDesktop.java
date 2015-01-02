/**
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
package org.neo4j.desktop;

import java.net.URISyntaxException;

import org.neo4j.desktop.config.DefaultDirectories;
import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.runtime.DatabaseActions;
import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.MainWindow;
import org.neo4j.desktop.ui.PlatformUI;

import static org.neo4j.desktop.ui.Components.alert;

/**
 * The main class for starting the Neo4j desktop app window. The different components and wired up and started.
 */
public final class Neo4jDesktop
{
    public static void main( String[] args )
    {
        new Neo4jDesktop().start();
    }

    private void start()
    {
        PlatformUI.selectPlatformUI();

        Environment environment;
        try
        {
            environment = new Environment();
        }
        catch ( URISyntaxException e )
        {
            alert( e.getMessage() );
            e.printStackTrace( System.out );
            return;
        }

        DesktopModel model = new DesktopModel( environment, DefaultDirectories.defaultDatabaseDirectory() );
        DatabaseActions databaseActions = new DatabaseActions( model );
        addShutdownHook( databaseActions );

        MainWindow window = new MainWindow( databaseActions, environment, model );
        window.display();
    }

    protected void addShutdownHook( final DatabaseActions databaseActions )
    {
        Runtime.getRuntime()
                .addShutdownHook( new Thread()
                {
                    @Override
                    public void run()
                    {
                        databaseActions.stop();
                    }
                } );
    }
}
