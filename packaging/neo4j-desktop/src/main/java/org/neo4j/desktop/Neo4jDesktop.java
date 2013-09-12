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
package org.neo4j.desktop;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.OperatingSystemFamily;
import org.neo4j.desktop.runtime.DatabaseActions;
import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.MainWindow;

import static java.lang.String.format;
import static javax.swing.filechooser.FileSystemView.getFileSystemView;

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
        selectPlatformUI();

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

        DesktopModel model = new DesktopModel( environment, defaultDatabaseDirectory() );
        DatabaseActions databaseActions = new DatabaseActions( model );

        MainWindow window = new MainWindow( databaseActions, environment, model );
        window.display();
    }


    private void selectPlatformUI()
    {
        try
        {
            UIManager.setLookAndFeel( UIManager.getSystemLookAndFeelClassName() );
        }
        catch ( ClassNotFoundException e)
        {
            // don't care
            e.printStackTrace( System.out );
        }
        catch ( UnsupportedLookAndFeelException e )
        {
            // don't care
            e.printStackTrace( System.out );
        }
        catch ( InstantiationException e )
        {
            // don't care
            e.printStackTrace( System.out );
        }
        catch ( IllegalAccessException e )
        {
            // don't care
            e.printStackTrace( System.out );
        }
    }

    private File defaultDatabaseDirectory()
    {
        return ensureIsDirectory( "Neo4j database directory",
            new File( defaultNeo4jDataDirectory(), "default.graphdb" ) );
    }

    private File defaultNeo4jDataDirectory()
    {
        ArrayList<File> locations = new ArrayList<File>(  );

        switch ( OperatingSystemFamily.detect() )
        {
            case WINDOWS:
                // cf. http://stackoverflow.com/questions/1503555/how-to-find-my-documents-folder
                locations.add( getFileSystemView().getDefaultDirectory() );
                break;
            case MAC_OS:
                // cf. http://stackoverflow.com/questions/567874/how-do-i-find-the-users-documents-folder-with-java-in-os-x
                locations.add( new File( new File( System.getProperty( "user.home" ) ), "Documents" ) );
                break;
        }

        locations.add( new File( System.getProperty( "user.home" ) ) );

        File documents = selectFirstWritableDirectoryOrElse( locations, new File( System.getProperty( "user.dir" ) ) );
        File neo4jData = new File( documents, "Neo4j" );

        return ensureIsDirectory( "Neo4j data directory", neo4jData );
    }

    private File selectFirstWritableDirectoryOrElse( List<File> locations, File defaultFile )
    {
        File result = defaultFile.getAbsoluteFile();
        for ( File file : locations )
        {
            File candidateFile = file.getAbsoluteFile();
            if ( candidateFile.exists() && candidateFile.isDirectory() && candidateFile.canWrite() ) {
                result = candidateFile;
                break;
            }
        }
        return result;
    }

    private File ensureIsDirectory( String description, File file )
    {
        if ( file.exists() )
        {
            if ( !file.isDirectory() )
            {
                alert( format( "%s already exists but is not a %s.", description, file.getAbsolutePath() ) );
            }
        }
        else if ( !file.mkdir() )
        {
            alert( format( "Could not make %s %s", description, file.getAbsolutePath() ) );
        }
        return file;
    }
}
