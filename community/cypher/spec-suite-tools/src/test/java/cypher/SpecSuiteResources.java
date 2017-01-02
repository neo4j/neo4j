/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
    public SpecSuiteResources( Class<?> klass, RunnerBuilder builder ) throws Throwable
    {
        super( klass, builder );
    }

    protected static RunnerBuilder download( Class<?> klass, RunnerBuilder builder )
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
                findAndUnpackTo( klass, fileSystem, path, featuresDirectory, graphsDirectory );
            }
            catch (IllegalArgumentException e)
            {
                // This is a workaround as the JDK doesn't give us a filesystem for subdirectories
                if ( "file".equals( uri.getScheme() ) )
                {
                    Path path = new File( uri.getPath() ).toPath();
                    findAndUnpackTo( klass, FileSystems.getDefault(), path, featuresDirectory, graphsDirectory );
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
            Class<?> klass,
            FileSystem fileSystem,
            Path sourceRootDirectory,
            File featuresDirectory,
            File graphsDirectory
    ) throws IOException
    {
        findAndUnpackTo( klass, fileSystem, sourceRootDirectory, "glob:**/*.feature", featuresDirectory );
        findAndUnpackTo( klass, fileSystem, sourceRootDirectory, "glob:**/*.cypher", graphsDirectory );
        findAndUnpackTo( klass, fileSystem, sourceRootDirectory, "glob:**/*.json", graphsDirectory );
    }

    private static void findAndUnpackTo(
            Class<?> klass,
            FileSystem sourceFileSystem,
            Path sourceRootDirectory,
            String sourceFilePattern,
            File targetDirectory
    ) throws IOException
    {
        System.out.println( "Unpacking to " + targetDirectory.getCanonicalPath() );
        PathMatcher matcher = sourceFileSystem.getPathMatcher( sourceFilePattern );
        boolean replaceExisting = isReplacingExistingFeatureFiles( klass );
        for (Iterator<Path> it = Files.walk( sourceRootDirectory, 3 ).iterator(); it.hasNext(); )
        {
            Path next = it.next();
            if ( matcher.matches( next ) )
            {
                File target = new File( targetDirectory, next.getFileName().toString() );
                if ( replaceExisting )
                    Files.copy( next, target.toPath(), StandardCopyOption.REPLACE_EXISTING );
                else
                    Files.copy( next, target.toPath() );
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

    protected static URI getResourceUri( Class<?> klass ) throws URISyntaxException
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

    private static boolean isReplacingExistingFeatureFiles( Class<?> klass )
    {
        try
        {
            return klass.getDeclaredField( "REPLACE_EXISTING" ).getBoolean( null );
        }
        catch ( IllegalAccessException | NoSuchFieldException e )
        {
            throw new RuntimeException( e );
        }
    }
}
