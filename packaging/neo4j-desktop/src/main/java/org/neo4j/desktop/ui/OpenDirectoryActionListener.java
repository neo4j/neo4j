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

import java.awt.Component;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedMessageDialog;

public class OpenDirectoryActionListener implements ActionListener
{
    private final Component parent;
    private final File directory;
    private final DesktopModel model;

    public OpenDirectoryActionListener( Component parent, File directory, DesktopModel model )
    {
        this.parent = parent;
        this.directory = directory;
        this.model = model;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        if ( isExistingDirectory( directory ) || directory.mkdirs() )
        {
            try
            {
                model.openDirectory( directory );
            }
            catch ( IOException exception )
            {
                String message =
                        "Could not open directory or create directory: " + directory + "\n\n" + exception.getMessage();
                showError( message );
            }
        }
        else
        {
            String message = "Could not open directory or create directory: " + directory;
            showError( message );
        }
    }

    private void showError( String message )
    {
        showWrappedMessageDialog( parent, message, "Error", ERROR_MESSAGE );
    }

    private boolean isExistingDirectory( File directory )
    {
        return directory.exists() && directory.isDirectory();
    }
}
