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
package org.neo4j.desktop.ui;

import org.apache.commons.lang3.SystemUtils;

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import javax.swing.*;

import org.neo4j.desktop.model.DesktopModel;
import org.neo4j.desktop.model.LastLocation;
import org.neo4j.desktop.model.exceptions.UnsuitableDirectoryException;

import static javax.swing.JFileChooser.APPROVE_OPTION;
import static javax.swing.JFileChooser.CUSTOM_DIALOG;
import static javax.swing.JFileChooser.DIRECTORIES_ONLY;
import static javax.swing.JOptionPane.CANCEL_OPTION;
import static javax.swing.JOptionPane.ERROR_MESSAGE;
import static javax.swing.JOptionPane.OK_CANCEL_OPTION;
import static org.neo4j.desktop.ui.ScrollableOptionPane.showWrappedConfirmDialog;

class BrowseForDatabaseActionListener implements ActionListener
{
    private final JFrame frame;
    private final JTextField directoryDisplay;
    private final DesktopModel model;

    public BrowseForDatabaseActionListener( JFrame frame, JTextField directoryDisplay, DesktopModel model )
    {
        this.model = model;
        this.frame = frame;
        this.directoryDisplay = directoryDisplay;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        File selectedFile = null;
        boolean cancelled = false;
        boolean validLocation = false;

        while ( !validLocation && !cancelled )
        {
            if ( SystemUtils.IS_OS_MAC )
            {
                selectedFile = macFileSelection();
            }
            else
            {
                selectedFile = fileSelection();
            }

            try
            {
                model.setDatabaseDirectory( selectedFile );
                directoryDisplay.setText( selectedFile.getAbsolutePath() );

                validLocation = true;

                LastLocation.setLastLocation( selectedFile.getAbsolutePath() );
            }
            catch ( UnsuitableDirectoryException ude )
            {
                int choice = showWrappedConfirmDialog( frame, "Please choose a different folder." + "\n" + ude.getStackTrace(),
                        "Invalid folder selected", OK_CANCEL_OPTION, ERROR_MESSAGE );

                if ( choice == CANCEL_OPTION )
                {
                    cancelled = true;
                }
            }
        }
    }

    private File fileSelection()
    {
        File selectedFile = null;

        JFileChooser fileChooser = new JFileChooser();
        fileChooser.setFileSelectionMode( DIRECTORIES_ONLY );
        fileChooser.setCurrentDirectory( new File( directoryDisplay.getText() ) );
        fileChooser.setDialogTitle( "Select database" );
        fileChooser.setDialogType( CUSTOM_DIALOG );

        int choice = fileChooser.showOpenDialog( frame );

        if ( choice == APPROVE_OPTION )
        {
            selectedFile = fileChooser.getSelectedFile();
        }

        return selectedFile;
    }

    private File macFileSelection()
    {
        System.setProperty( "apple.awt.fileDialogForDirectories", "true" );
        FileDialog fileDialog = new FileDialog( frame );

        fileDialog.setDirectory( directoryDisplay.getText() );
        fileDialog.setVisible( true );

        File selectedFile = new File( fileDialog.getDirectory(), fileDialog.getFile() );
        System.setProperty( "apple.awt.fileDialogForDirectories", "false" );

        return selectedFile;
    }
}
