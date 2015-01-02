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
package org.neo4j.desktop.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static javax.swing.filechooser.FileSystemView.getFileSystemView;

import static org.neo4j.desktop.ui.Components.alert;

public class DefaultDirectories
{
    public static File defaultDatabaseDirectory()
    {
        return ensureIsDirectory( "Neo4j database directory",
            new File( defaultNeo4jDataDirectory(), "default.graphdb" ) );
    }

    private static File defaultNeo4jDataDirectory()
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

    private static File selectFirstWritableDirectoryOrElse( List<File> locations, File defaultFile )
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

    private static File ensureIsDirectory( String description, File file )
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
