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

import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.swing.*;

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
        this.frame = frame;
        this.directoryDisplay = directoryDisplay;
        this.model = model;
    }

    @Override
    public void actionPerformed( ActionEvent e )
    {
        File selectedFile = null;
        boolean cancelled = false;
        boolean validLocation = false;
        String os = System.getProperty( "os.name" );

        while ( !validLocation && !cancelled )
        {
            if ( os.toLowerCase().contains( "mac" ) )
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
                directoryDisplay.setText( model.getDatabaseDirectory().getAbsolutePath() );

                validLocation = true;

                FileWriter fileWriter = new FileWriter( new File( ".dblocation" ) );
                fileWriter.write( selectedFile.getAbsolutePath() );
                fileWriter.flush();
                fileWriter.close();
            }
            catch ( UnsuitableDirectoryException ude )
            {
                int choice = showWrappedConfirmDialog(
                        frame,
                        ude.getMessage() + "\nPlease choose a different folder.",
                        "Invalid folder selected", OK_CANCEL_OPTION, ERROR_MESSAGE );

                if ( choice == CANCEL_OPTION )
                {
                    cancelled = true;
                }
            }
            catch ( IOException ioe )
            {
                System.out.println( "Error saving DB location" );
                System.out.println( ioe );
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
        File selectedFile = null;

        System.setProperty( "apple.awt.fileDialogForDirectories", "true" );
        FileDialog fileDialog = new FileDialog( frame );

        fileDialog.setDirectory( directoryDisplay.getText() );
        fileDialog.setVisible( true );

        selectedFile = new File( fileDialog.getFile() );
        System.setProperty( "apple.awt.fileDialogForDirectories", "false" );

        return selectedFile;
    }


    public static void main( String[] args )
    {
        System.out.println( System.getProperty( "os.name" ) );
    }

}
