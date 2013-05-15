/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;

public class TargetDirectory
{
    public class TestDirectory implements TestRule
    {
        private final boolean clean;
        private File subdir = null;

        @Deprecated
        public TestDirectory()
        {
            this( false );
        }

        private TestDirectory( boolean clean )
        {
            this.clean = clean;
        }

        public File directory()
        {
            if ( subdir == null ) throw new IllegalStateException( "Not initialized" );
            return subdir;
        }

        @Override
        public Statement apply( final Statement base, Description description )
        {
            subdir = TargetDirectory.this.directory( description.getMethodName(), clean );
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

        protected void complete( boolean success )
        {
            if ( success && subdir != null ) recursiveDelete( subdir );
            subdir = null;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "["
                   + ( subdir == null ? "<uninitialized>" : subdir.toString() ) + "]";
        }
    }

    private final FileSystemAbstraction fileSystem;
    private final File base;

    private TargetDirectory( FileSystemAbstraction fileSystem, File base )
    {
        this.fileSystem = fileSystem;
        this.base = base.getAbsoluteFile();
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

    public File directory( String name )
    {
        return directory( name, false );
    }

    public File directory( String name, boolean clean )
    {
        File dir = new File( base(), name );
        if ( clean && fileSystem.fileExists( dir ) )
        {
            recursiveDelete( dir );
        }
        fileSystem.mkdir( dir );
        return dir;
    }

    public File file( String name )
    {
        return new File( base(), name );
    }

    private File base()
    {
        if ( fileSystem.fileExists( base ) )
        {
            if ( !fileSystem.isDirectory( base ) )
                throw new IllegalStateException( base + " exists and is not a directory!" );
        }
        else
        {
            try
            {
                fileSystem.mkdirs( base );
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
        return base;
    }

    public TestDirectory testDirectory()
    {
        return new TestDirectory( false );
    }

    public TestDirectory cleanTestDirectory()
    {
        return new TestDirectory( true );
    }

    public static TargetDirectory forTest( Class<?> owningTest )
    {
        return forTest( new DefaultFileSystemAbstraction(), owningTest );
    }

    public static TargetDirectory forTest( FileSystemAbstraction fileSystem, Class<?> owningTest )
    {
        File target = null;
        try
        {
            File codeSource = new File(
                    owningTest.getProtectionDomain().getCodeSource().getLocation().toURI() );
            if ( codeSource.exists() )
            {
                if ( codeSource.isFile() )// jarfile
                {
                }
                else if ( codeSource.isDirectory() )// classes dir
                {
                    target = codeSource.getParentFile();
                }
            }
        }
        catch ( URISyntaxException e )
        {
        }
        if ( target == null )
        {
            target = new File( "target" );
        }
        return new TargetDirectory( fileSystem,
                new File( new File( target, "test-data" ), owningTest.getName() ) );
    }

    public static TestDirectory testDirForTest( FileSystemAbstraction fileSystem, Class<?> owningTest )
    {
        return forTest( fileSystem, owningTest ).testDirectory();
    }

    public static TestDirectory testDirForTest( Class<?> owningTest )
    {
        return testDirForTest( new DefaultFileSystemAbstraction(), owningTest );
    }

    public File graphDbDir( boolean clean )
    {
        return directory( "graph-db", clean );
    }

    public void cleanup() throws IOException
    {
        fileSystem.deleteRecursively( base );
        fileSystem.mkdirs( base );
    }
}
