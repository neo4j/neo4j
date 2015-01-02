/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.util.FileUtils;

import static java.lang.String.format;

public class TargetDirectory
{
    public class TestDirectory implements TestRule
    {
        private File subdir = null;

        private TestDirectory() { }

        public String absolutePath()
        {
            return directory().getAbsolutePath();
        }

        public File directory()
        {
            if ( subdir == null ) throw new IllegalStateException( "Not initialized" );
            return subdir;
        }

        @Override
        public Statement apply( final Statement base, Description description )
        {
            subdir = directoryForDescription( description );
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    boolean success = false;
                    try
                    {
                        base.evaluate();
                        success = true;
                    }
                    finally
                    {
                        complete( success );
                    }
                }
            };
        }

        @Override
        public String toString()
        {
            String subdirName = subdir == null ? "<uninitialized>" : subdir.toString();
            return format( "%s[%s]", getClass().getSimpleName(), subdirName );
        }

        private void complete( boolean success )
        {
            if ( success && subdir != null )
            {
                try
                {
                    recursiveDelete( subdir );
                }
                catch ( RuntimeException e )
                {
                    if ( e.getCause() != null &&
                            e.getCause() instanceof FileUtils.MaybeWindowsMemoryMappedFileReleaseProblem )
                    {
                        System.err.println( "Failed to delete test directory, maybe due to Windows memory-mapped file problem" );
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
            subdir = null;
        }
    }

    private final FileSystemAbstraction fileSystem;
    private final File base;

    public static TargetDirectory forTest( Class<?> owningTest )
    {
        //noinspection deprecation
        return forTest( new DefaultFileSystemAbstraction(), owningTest );
    }

    public static TargetDirectory forTest( FileSystemAbstraction fileSystem, Class<?> owningTest )
    {
        return new TargetDirectory( fileSystem,
                new File( new File( locateTarget( owningTest ), "test-data" ), owningTest.getName() ) );
    }

    public static TestDirectory testDirForTest( Class<?> owningTest )
    {
        //noinspection deprecation
        return forTest( new DefaultFileSystemAbstraction(), owningTest ).testDirectory();
    }

    private TargetDirectory( FileSystemAbstraction fileSystem, File base )
    {
        this.fileSystem = fileSystem;
        this.base = base.getAbsoluteFile();
    }

    public File cacheDirectory( String name )
    {
        File dir = new File( ensureBase(), name );
        if ( ! fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdir( dir );
        }
        return dir;
    }

    public File existingDirectory( String name )
    {
        return new File( base, name );
    }

    public File cleanDirectory( String name )
    {
        File dir = new File( ensureBase(), name );
        if ( fileSystem.fileExists( dir ) )
        {
            recursiveDelete( dir );
        }
        fileSystem.mkdir( dir );
        return dir;
    }

    public File directoryForDescription( Description description )
    {
        String test = description.getMethodName();
        String dir = DigestUtils.md5Hex( test );
        register( test, dir );
        return cleanDirectory( dir );
    }

    public File file( String name )
    {
        return new File( ensureBase(), name );
    }

    public TestDirectory testDirectory()
    {
        return new TestDirectory();
    }

    public File makeGraphDbDir()
    {
        return cleanDirectory( "graph-db" );
    }

    public void cleanup() throws IOException
    {
        fileSystem.deleteRecursively( base );
        fileSystem.mkdirs( base );
    }

    private void recursiveDelete( File file )
    {
        try
        {
            fileSystem.deleteRecursively( file );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void register( String test, String dir )
    {
        try
        {
            FileUtils.writeToFile( new File( ensureBase(), ".register" ), format( "%s=%s\n", dir, test ), true );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private File ensureBase()
    {
        if ( fileSystem.fileExists( base ) && !fileSystem.isDirectory( base ) )
            throw new IllegalStateException( base + " exists and is not a directory!" );

        try
        {
            fileSystem.mkdirs( base );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return base;
    }

    private static File locateTarget( Class<?> owningTest )
    {
        try
        {
            File codeSource = new File( owningTest.getProtectionDomain().getCodeSource().getLocation().toURI() );
            if ( codeSource.isDirectory() )
            {
                // code loaded from a directory
                return codeSource.getParentFile();
            }
        }
        catch ( URISyntaxException e )
        {
            // ignored
        }
        return new File( "target" );
    }
}
