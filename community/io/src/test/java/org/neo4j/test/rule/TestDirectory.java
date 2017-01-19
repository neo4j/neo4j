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
package org.neo4j.test.rule;

import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.FileUtils.MaybeWindowsMemoryMappedFileReleaseProblem;
import org.neo4j.test.Digests;

import static java.lang.String.format;

/**
 * This class defines a JUnit rule which ensures that the test's working directory is cleaned up. The clean-up
 * only happens if the test passes, to help diagnose test failures.  For example:
 * <pre>
 *   public class SomeTest
 *   {
 *     @Rule
 *     public TestDirectory dir = TestDirectory.testDirectory();
 *
 *     @Test
 *     public void shouldDoSomething()
 *     {
 *       File storeDir = dir.graphDbDir();
 *       // do stuff with store dir
 *     }
 *   }
 * </pre>
 */
public class TestDirectory implements TestRule
{
    private final FileSystemAbstraction fileSystem;
    private File testClassBaseFolder;
    private Class<?> owningTest;
    private boolean keepDirectoryAfterSuccessfulTest;
    private File testDirectory = null;

    private TestDirectory( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    private TestDirectory( FileSystemAbstraction fileSystem, Class<?> owningTest )
    {
        this.fileSystem = fileSystem;
        this.owningTest = owningTest;
    }

    public static TestDirectory testDirectory()
    {
        return new TestDirectory( new DefaultFileSystemAbstraction() );
    }

    public static TestDirectory testDirectory( FileSystemAbstraction fs )
    {
        return new TestDirectory( fs );
    }

    public static TestDirectory testDirectory( Class<?> owningTest )
    {
        return new TestDirectory( new DefaultFileSystemAbstraction(), owningTest );
    }

    public static TestDirectory testDirectory( Class<?> owningTest, FileSystemAbstraction fs )
    {
        return new TestDirectory( fs, owningTest );
    }

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                testDirectory = directoryForDescription( description );
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

    /**
     * Tell this {@link Rule} to keep the store directory, even after a successful test.
     * It's just a useful debug mechanism to have for analyzing store after a test.
     * by default directories aren't kept.
     */
    public TestDirectory keepDirectoryAfterSuccessfulTest()
    {
        keepDirectoryAfterSuccessfulTest = true;
        return this;
    }

    public File absolutePath()
    {
        return directory().getAbsoluteFile();
    }

    public File directory()
    {
        if ( testDirectory == null )
        {
            throw new IllegalStateException( "Not initialized" );
        }
        return testDirectory;
    }

    public File directory( String name )
    {
        File dir = new File( directory(), name );
        if ( !fileSystem.fileExists( dir ) )
        {
            fileSystem.mkdir( dir );
        }
        return dir;
    }

    public File file( String name )
    {
        return new File( directory(), name );
    }

    public File graphDbDir()
    {
        return directory( "graph-db" );
    }

    public File makeGraphDbDir() throws IOException
    {
        return cleanDirectory( "graph-db" );
    }

    public void cleanup() throws IOException
    {
        clean( fileSystem, testClassBaseFolder );
    }

    @Override
    public String toString()
    {
        String testDirectoryName = testDirectory == null ? "<uninitialized>" : testDirectory.toString();
        return format( "%s[%s]", getClass().getSimpleName(), testDirectoryName );
    }

    public File cleanDirectory( String name ) throws IOException
    {
        return clean( fileSystem, new File( ensureBase(), name ) );
    }

    private static File clean( FileSystemAbstraction fs, File dir ) throws IOException
    {
        if ( fs.fileExists( dir ) )
        {
            fs.deleteRecursively( dir );
        }
        fs.mkdirs( dir );
        return dir;
    }

    private void complete( boolean success )
    {
        if ( success && testDirectory != null && !keepDirectoryAfterSuccessfulTest )
        {
            try
            {
                fileSystem.deleteRecursively( testDirectory );
            }
            catch ( MaybeWindowsMemoryMappedFileReleaseProblem fme )
            {
                System.err.println( "Failed to delete test directory, " +
                                    "maybe due to Windows memory-mapped file problem: " + fme.getMessage() );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        testDirectory = null;
    }

    private File directoryForDescription( Description description ) throws IOException
    {
        if ( owningTest == null )
        {
            owningTest = description.getTestClass();
        }
        String test = description.getMethodName();
        if ( test == null )
        {
            test = "static";
        }
        return prepareDirectoryForTest( test );
    }

    public File prepareDirectoryForTest( String test ) throws IOException
    {
        String dir = Digests.md5Hex( test );
        evaluateClassBaseTestFolder();
        register( test, dir );
        return cleanDirectory( dir );
    }

    private void evaluateClassBaseTestFolder( )
    {
        if (owningTest == null)
        {
            throw new IllegalStateException(" Test owning class is not defined" );
        }
        try
        {
            testClassBaseFolder = testDataDirectoryOf( fileSystem, owningTest, false );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static File testDataDirectoryOf( FileSystemAbstraction fs, Class<?> owningTest, boolean clean )
            throws IOException
    {
        File testData = new File( locateTarget( owningTest ), "test-data" );
        File result = new File( testData, shorten( owningTest.getName() ) ).getAbsoluteFile();
        if ( clean )
        {
            clean( fs, result );
        }
        return result;
    }

    private static String shorten( String owningTestName )
    {
        int targetPartLength = 5;
        String[] parts = owningTestName.split( "\\." );
        for ( int i = 0; i < parts.length - 1; i++ )
        {
            String part = parts[i];
            if ( part.length() > targetPartLength )
            {
                parts[i] = part.substring( 0, targetPartLength - 1 ) + "~";
            }
        }
        return Arrays.stream( parts ).collect( Collectors.joining( "." ) );
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
        if ( testClassBaseFolder == null )
        {
            evaluateClassBaseTestFolder();
        }
        if ( fileSystem.fileExists( testClassBaseFolder ) && !fileSystem.isDirectory( testClassBaseFolder ) )
        {
            throw new IllegalStateException( testClassBaseFolder + " exists and is not a directory!" );
        }

        try
        {
            fileSystem.mkdirs( testClassBaseFolder );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        return testClassBaseFolder;
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
