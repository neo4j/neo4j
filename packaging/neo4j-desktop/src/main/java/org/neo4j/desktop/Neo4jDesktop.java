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
import java.util.ArrayList;
import java.util.List;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import org.neo4j.desktop.config.Environment;
import org.neo4j.desktop.config.OsSpecificEnvironment;
import org.neo4j.desktop.config.OsSpecificExtensionPackagesConfig;
import org.neo4j.desktop.config.Value;
import org.neo4j.desktop.runtime.DatabaseActions;
import org.neo4j.desktop.ui.DesktopModel;
import org.neo4j.desktop.ui.MainWindow;

import static javax.swing.filechooser.FileSystemView.getFileSystemView;

/**
 * The main class for starting the Neo4j desktop app window. The different components and wired up and started.
 */
public class Neo4jDesktop
{
    public static void main( String[] args )
    {
        new Neo4jDesktop().start();
    }

    private void start()
    {
        selectPlatformUI();

        Environment environment = new OsSpecificEnvironment().get();

        Value<List<String>> extensionPackagesConfig =
                new OsSpecificExtensionPackagesConfig( environment ).get();

        DesktopModel model = new DesktopModel( defaultDatabaseDirectory(), extensionPackagesConfig );

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
        catch ( ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException e )
        {
            // don't care
        }
    }

    private File defaultDatabaseDirectory()
    {
        ArrayList<File> locations = new ArrayList<>(  );

        // Works according to: http://www.osgi.org/Specifications/Reference
        String os = System.getProperty( "os.name" );

        if ( os.startsWith( "Windows" ) )
        {

            // cf. http://stackoverflow.com/questions/1503555/how-to-find-my-documents-folder
            locations.add( getFileSystemView().getDefaultDirectory() );
        }

        if ( os.startsWith( "Mac OS" ) )
        {
            // cf. http://stackoverflow.com/questions/567874/how-do-i-find-the-users-documents-folder-with-java-in-os-x
            locations.add( new File( new File( System.getProperty( "user.home" ) ), "Documents" ) );
        }

        locations.add( new File( System.getProperty( "user.home" ) ) );

        File result = selectFirstWritableDirectoryOrElse( locations, new File( System.getProperty( "user.dir" ) ) );
        return new File( result, "neo4j" );
    }

    private File selectFirstWritableDirectoryOrElse( ArrayList<File> locations, File defaultFile )
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
}
