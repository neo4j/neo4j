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
package org.neo4j.desktop.ui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.neo4j.desktop.runtime.DatabaseActions;

import static javax.swing.SwingUtilities.invokeLater;

import static org.neo4j.desktop.ui.Components.alert;
import static org.neo4j.desktop.ui.DatabaseStatus.STARTED;
import static org.neo4j.desktop.ui.DatabaseStatus.STARTING;
import static org.neo4j.desktop.ui.DatabaseStatus.STOPPED;

class StartDatabaseActionListener implements ActionListener
{
    private MainWindow mainWindow;
    private final DesktopModel model;
    private final DatabaseActions databaseActions;

    public StartDatabaseActionListener( MainWindow mainWindow, DesktopModel model, DatabaseActions databaseActions )
    {
        this.mainWindow = mainWindow;
        this.model = model;
        this.databaseActions = databaseActions;
    }

    @Override
    public void actionPerformed( ActionEvent event )
    {
        mainWindow.updateStatus( STARTING );

        invokeLater( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    model.prepareGraphDirectoryForStart();

                    databaseActions.start();
                    mainWindow.updateStatus( STARTED );
                }
                catch ( UnsuitableDirectoryException | UnableToStartServerException e )
                {
                    updateUserWithErrorMessageAndStatus( e );
                }
            }

            private void updateUserWithErrorMessageAndStatus( Exception e )
            {
                alert( e.getMessage() );
                mainWindow.updateStatus( STOPPED );
            }
        } );
    }
}
