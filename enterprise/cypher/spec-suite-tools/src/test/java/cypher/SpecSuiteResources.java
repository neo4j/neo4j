/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher;

import org.junit.experimental.runners.Enclosed;
import org.junit.runners.model.RunnerBuilder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Iterator;

public abstract class SpecSuiteResources extends Enclosed
{
    SpecSuiteResources( Class<?> klass, RunnerBuilder builder ) throws Throwable
    {
        super( klass, builder );
    }

    static RunnerBuilder download( Class<?> klass, RunnerBuilder builder )
    {
        unpackResources( klass );
        return builder;
    }

    private static void unpackResources( Class<?> klass )
    {
        String specSuiteName = getSpecSuiteName( klass );
        File featuresDirectory = createTargetDirectory( specSuiteName,"features" );
        File graphsDirectory = createTargetDirectory( specSuiteName, "graphs" );

        URI uri;
        try
        {
            uri = getResourceUri( klass );
        }
        catch ( URISyntaxException e )
        {
            throw new IllegalStateException( "Failed to find resources for TCK feature files in JAR!", e );
        }
        try
        {
            try ( FileSystem fileSystem = FileSystems.newFileSystem( uri, Collections.emptyMap() ) )
            {
                Path path = fileSystem.getPath( "/" );
                findAndUnpackTo( fileSystem, path, featuresDirectory, graphsDirectory );
            }
            catch (IllegalArgumentException e)
            {
                // This is a workaround as the JDK doesn't give us a filesystem for subdirectories
                if ( "file".equals( uri.getScheme() ) )
                {
                    Path path = new File( uri.getPath() ).toPath();
                    findAndUnpackTo( FileSystems.getDefault(), path, featuresDirectory, graphsDirectory );
                }
                else
                {
                    throw e;
                }
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Unexpected error while unpacking Cypher TCK feature files", e );
        }
    }

    private static void findAndUnpackTo(
            FileSystem fileSystem,
            Path sourceRootDirectory,
            File featuresDirectory,
            File graphsDirectory
    ) throws IOException
    {
        findAndUnpackTo( fileSystem, sourceRootDirectory, "glob:**/*.feature", featuresDirectory );
        findAndUnpackTo( fileSystem, sourceRootDirectory, "glob:**/*.cypher", graphsDirectory );
        findAndUnpackTo( fileSystem, sourceRootDirectory, "glob:**/*.json", graphsDirectory );
    }

    private static void findAndUnpackTo(
            FileSystem sourceFileSystem,
            Path sourceRootDirectory,
            String sourceFilePattern,
            File targetDirectory
    ) throws IOException
    {
        System.out.println( "Unpacking to " + targetDirectory.getCanonicalPath() );
        PathMatcher matcher = sourceFileSystem.getPathMatcher( sourceFilePattern );
        for (Iterator<Path> it = Files.walk( sourceRootDirectory, 3 ).iterator(); it.hasNext(); )
        {
            Path next = it.next();
            if ( matcher.matches( next ) )
            {
                File target = new File( targetDirectory, next.getFileName().toString() );
                Files.copy( next, target.toPath(), StandardCopyOption.REPLACE_EXISTING );
                System.out.println( "Unpacked " + target.getName() );
            }
        }
    }

    private static File createTargetDirectory( String specSuiteName, String suffix )
    {
        return obtainTargetDirectory( true, specSuiteName, suffix );
    }

    public static File targetDirectory( Class<?> klass, String suffix )
    {
        return obtainTargetDirectory( false, getSpecSuiteName( klass ), suffix );
    }

    private static File obtainTargetDirectory( boolean create, String specSuiteName, String suffix )
    {
        File directory = new File( new File( new File( "target" ), specSuiteName ), suffix ).getAbsoluteFile();
        if ( !directory.exists() )
        {
            if ( !(create && directory.mkdirs()) )
            {
                throw new IllegalStateException(
                        "Failed to create target directory for cypher feature files: " + directory );
            }
        }
        return directory;
    }

    private static URI getResourceUri( Class<?> klass ) throws URISyntaxException
    {
        return getResourceUriClass( klass ).getResource( "" ).toURI();
    }

    private static Class<?> getResourceUriClass( Class<?> klass )
    {
        try
        {
            return (Class<?>) klass.getDeclaredField( "RESOURCE_CLASS" ).get( null );
        }
        catch ( IllegalAccessException | NoSuchFieldException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static String getSpecSuiteName( Class<?> klass )
    {
        try
        {
            return (String) klass.getDeclaredField( "SUITE_NAME" ).get( null );
        }
        catch ( IllegalAccessException | NoSuchFieldException e )
        {
            throw new RuntimeException( e );
        }
    }
}
