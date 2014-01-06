/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.io.File;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.showConfirmDialog;

class BrowseForDatabaseActionListener implements ActionListener
{
    private final JFrame frame;
    private final JTextField directoryDisplay;
    private final DesktopModel model;

    public BrowseForDatabaseActionListener( JFrame frame, JTextField directoryDisplay, DesktopModel model )
    {
        this.frame = frame;
        this.directoryDisplay = directoryDisplay;
        this.model = model;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        JFileChooser jFileChooser = new JFileChooser();
        jFileChooser.setFileSelectionMode( DIRECTORIES_ONLY );
        jFileChooser.setCurrentDirectory( new File( directoryDisplay.getText() ) );
        jFileChooser.setDialogTitle( "Select database" );
        jFileChooser.setDialogType( CUSTOM_DIALOG );

        while ( true )
        {
            int choice = jFileChooser.showOpenDialog( frame );

            if ( choice != APPROVE_OPTION )
            {
                return;
            }

            File selectedFile = jFileChooser.getSelectedFile();
            try
            {
                model.setDatabaseDirectory( selectedFile );
                directoryDisplay.setText( model.getDatabaseDirectory().getAbsolutePath() );
                return;
            }
            catch ( UnsuitableDirectoryException error )
            {
                int result = showConfirmDialog(
                        frame, error.getMessage() + "\nPlease choose a different folder.",
                        "Invalid folder selected", JOptionPane.OK_CANCEL_OPTION );
                if ( result == CANCEL_OPTION )
                {
                    return;
                }
            }
        }
    }
}
