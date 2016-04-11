/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cypher;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.Iterator;

import org.junit.experimental.runners.Enclosed;
import org.junit.runners.model.RunnerBuilder;
import org.opencypher.tools.tck.TCKCucumberTemplate;

public class UnpackedFeatures extends Enclosed
{
    public UnpackedFeatures( Class<?> klass, RunnerBuilder builder ) throws Throwable
    {
        super( klass, download( builder ) );
    }

    private static RunnerBuilder download( RunnerBuilder builder )
    {
        unpackFeatures();
        return builder;
    }

    private static void unpackFeatures()
    {
        File directory = createTargetDirectory();

        URI uri;
        try
        {
            uri = TCKCucumberTemplate.class.getResource( "" ).toURI();
        }
        catch ( URISyntaxException e )
        {
            throw new IllegalStateException( "Failed to find resources for TCK feature files in JAR!", e );
        }
        try ( FileSystem fileSystem = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
        {
            PathMatcher matcher = fileSystem.getPathMatcher("glob:**/*.feature");
            for ( Iterator<Path> it = Files.walk( fileSystem.getPath( "/" ), 1 ).iterator(); it.hasNext(); )
            {
                Path next = it.next();
                if ( matcher.matches( next ) )
                {
                    File target = new File( directory, next.toString() );
                    Files.copy( next, target.toPath() );
                    System.out.println( "Unpacked " + target.getName() );
                }
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Unexpected error while unpacking Cypher TCK feature files", e );
        }
    }

    private static File createTargetDirectory()
    {
        File directory = new File( "target/features" );
        if ( !directory.exists() )
        {
            if ( !directory.mkdirs() )
            {
                throw new IllegalStateException(
                        "Failed to create target directory for cypher feature files: " + directory );
            }
        }
        return directory;
    }
}
