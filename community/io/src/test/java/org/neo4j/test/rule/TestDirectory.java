/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Rule;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.Random;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.util.VisibleForTesting;

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
 *       File storeDir = dir.databaseDir();
 *       // do stuff with store dir
 *     }
 *   }
 * </pre>
 */
public class TestDirectory extends ExternalResource
{
    private static final String DEFAULT_DATABASE_DIRECTORY = "graph.db";
    /**
     * This value is mixed into the hash string, along with the test name,
     * that we use for uniquely naming test directories.
     * By getting a new value here, every time the JVM is started, we the same
     * tests will get different directory names when executed many times in
     * different JVMs.
     * This way, the test results for many runs of the same tests are kept
     * around, so they can easily be compared with each other. This is useful
     * when you need to investigate a flaky test, for instance.
     */
    private static final long JVM_EXECUTION_HASH = new Random().nextLong();

    private final FileSystemAbstraction fileSystem;
    private File testClassBaseFolder;
    private Class<?> owningTest;
    private boolean keepDirectoryAfterSuccessfulTest;
    private File testDirectory;
    private StoreLayout storeLayout;
    private DatabaseLayout defaultDatabaseLayout;

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
                directoryForDescription( description );
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
        createDirectory( dir );
        return dir;
    }

    public File file( String name )
    {
        return new File( directory(), name );
    }

    public File createFile( String name )
    {
        File file = file( name );
        ensureFileExists( file );
        return file;
    }

    public File databaseDir()
    {
        return databaseLayout().databaseDirectory();
    }

    public StoreLayout storeLayout()
    {
        return storeLayout;
    }

    public DatabaseLayout databaseLayout()
    {
        createDirectory( defaultDatabaseLayout.databaseDirectory() );
        return defaultDatabaseLayout;
    }

    public DatabaseLayout databaseLayout( File storeDir )
    {
        DatabaseLayout databaseLayout = StoreLayout.of( storeDir ).databaseLayout( DEFAULT_DATABASE_DIRECTORY );
        createDirectory( databaseLayout.databaseDirectory() );
        return databaseLayout;
    }

    public DatabaseLayout databaseLayout( String name )
    {
        DatabaseLayout databaseLayout = storeLayout.databaseLayout( name );
        createDirectory( databaseLayout.databaseDirectory() );
        return databaseLayout;
    }

    public File storeDir()
    {
        return storeLayout.storeDirectory();
    }

    public File storeDir( String storeDirName )
    {
        return directory( storeDirName );
    }

    public File databaseDir( File storeDirectory )
    {
        File databaseDirectory = databaseLayout( storeDirectory ).databaseDirectory();
        createDirectory( databaseDirectory );
        return databaseDirectory;
    }

    public File databaseDir( String customStoreDirectoryName )
    {
        return databaseDir( storeDir( customStoreDirectoryName ) );
    }

    public void cleanup() throws IOException
    {
        clean( fileSystem, testClassBaseFolder );
    }

    @Override
    public String toString()
    {
        String testDirectoryName = testDirectory == null ? "<uninitialized>" : testDirectory.toString();
        return format( "%s[\"%s\"]", getClass().getSimpleName(), testDirectoryName );
    }

    public File cleanDirectory( String name ) throws IOException
    {
        return clean( fileSystem, new File( ensureBase(), name ) );
    }

    public void complete( boolean success ) throws IOException
    {
        try
        {
            if ( success && testDirectory != null && !keepDirectoryAfterSuccessfulTest )
            {
                fileSystem.deleteRecursively( testDirectory );
            }
            testDirectory = null;
            storeLayout = null;
            defaultDatabaseLayout = null;
        }
        finally
        {
            fileSystem.close();
        }
    }

    public void prepareDirectory( Class<?> testClass, String test ) throws IOException
    {
        if ( owningTest == null )
        {
            owningTest = testClass;
        }
        if ( test == null )
        {
            test = "static";
        }
        testDirectory = prepareDirectoryForTest( test );
        storeLayout = StoreLayout.of( testDirectory );
        defaultDatabaseLayout = storeLayout.databaseLayout( DEFAULT_DATABASE_DIRECTORY );
    }

    public File prepareDirectoryForTest( String test ) throws IOException
    {
        String dir = DigestUtils.md5Hex( JVM_EXECUTION_HASH + test );
        evaluateClassBaseTestFolder();
        register( test, dir );
        return cleanDirectory( dir );
    }

    @VisibleForTesting
    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    private void directoryForDescription( Description description ) throws IOException
    {
        prepareDirectory( description.getTestClass(), description.getMethodName() );
    }

    private void ensureFileExists( File file )
    {
        try
        {
            if ( !fileSystem.fileExists( file ) )
            {
                fileSystem.create( file ).close();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to create file: " + file, e );
        }
    }

    private void createDirectory( File databaseDirectory )
    {
        try
        {
            fileSystem.mkdirs( databaseDirectory );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Failed to create directory: " + databaseDirectory, e );
        }
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

    private void evaluateClassBaseTestFolder( )
    {
        if ( owningTest == null )
        {
            throw new IllegalStateException( " Test owning class is not defined" );
        }
        testClassBaseFolder = testDataDirectoryOf( owningTest );
    }

    private static File testDataDirectoryOf( Class<?> owningTest )
    {
        File testData = new File( locateTarget( owningTest ), "test data" );
        return new File( testData, shorten( owningTest.getName() ) ).getAbsoluteFile();
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
        return String.join( ".", parts );
    }

    private void register( String test, String dir )
    {
        try ( PrintStream printStream =
                    new PrintStream( fileSystem.openAsOutputStream( new File( ensureBase(), ".register" ), true ) ) )
        {
            printStream.print( format( "%s = %s%n", dir, test ) );
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
