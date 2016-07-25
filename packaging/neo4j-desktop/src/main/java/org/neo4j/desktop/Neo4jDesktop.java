/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.apache.commons.lang3.SystemUtils;

import org.neo4j.desktop.config.Installation;
import org.neo4j.desktop.config.osx.DarwinInstallation;
import org.neo4j.desktop.config.unix.UnixInstallation;
import org.neo4j.desktop.config.windows.WindowsInstallation;
import org.neo4j.desktop.model.DesktopModel;
import org.neo4j.desktop.runtime.DatabaseActions;
import org.neo4j.desktop.ui.MainWindow;
import org.neo4j.desktop.ui.PlatformUI;

import static org.neo4j.desktop.ui.Components.alert;

/**
 * The main class for starting the Neo4j desktop app window. The different components are wired up and started.
 */

public final class Neo4jDesktop
{
    public static void main( String[] args )
    {
        preStartInitialize();

        Neo4jDesktop app = new Neo4jDesktop( new Parameters( args ) );
        app.start();
    }

    private final Parameters parameters;

    public Neo4jDesktop( Parameters parameters )
    {
        this.parameters = parameters;
    }

    public static void preStartInitialize()
    {
        PlatformUI.selectPlatformUI();
        DesktopIdentification.register();
    }

    private void start()
    {
        try
        {
            Installation installation = getInstallation();
            installation.initialize();

            DesktopModel model = new DesktopModel( installation, parameters );
            DatabaseActions databaseActions = new DatabaseActions( model );
            addShutdownHook( databaseActions );

            MainWindow window = new MainWindow( databaseActions, model );
            window.display();
        }
        catch ( Exception e )
        {
            alert( e.getMessage() );
            e.printStackTrace( System.out );
        }
    }

    private Installation getInstallation() throws Exception
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            return new WindowsInstallation();
        }
        else if ( SystemUtils.IS_OS_MAC )
        {
            return new DarwinInstallation();
        }
        else
        {
            return new UnixInstallation();
        }
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
