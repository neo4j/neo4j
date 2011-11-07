/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

public class TargetDirectory
{
    public class TestDirectory implements TestRule
    {
        private File subdir = null;
        
        public File directory()
        {
            if ( subdir == null ) throw new IllegalStateException( "Not initialized" );
            return subdir;
        }

        @Override
        public Statement apply( final Statement base, Description description )
        {
            subdir = TargetDirectory.this.directory( description.getMethodName() );
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

    private final File base;

    private TargetDirectory( File base )
    {
        this.base = base.getAbsoluteFile();
    }

    public static void recursiveDelete( File file )
    {
        File[] files = file.listFiles();
        if ( files != null ) for ( File each : files )
            recursiveDelete( each );
        file.delete();
    }

    public File directory( String name )
    {
        return directory( name, false );
    }

    public File directory( String name, boolean clean )
    {
        File dir = new File( base(), name );
        if ( clean && dir.exists() ) recursiveDelete( dir );
        dir.mkdir();
        return dir;
    }

    public File file( String name )
    {
        return new File( base(), name );
    }

    private File base()
    {
        if ( base.exists() )
        {
            if ( !base.isDirectory() )
                throw new IllegalStateException( base + " exists and is not a directory!" );
        }
        else
        {
            base.mkdirs();
        }
        return base;
    }

    public TestDirectory testDirectory()
    {
        return new TestDirectory();
    }

    public static TargetDirectory forTest( Class<?> owningTest )
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
            if ( !( target.exists() && target.isDirectory() ) )
            {
                // Fall back to temporary directory
                try
                {
                    target = File.createTempFile( "neo4j-test", "target" );
                }
                catch ( IOException e )
                {
                    throw new IllegalStateException( "Cannot create target directory" );
                }
            }
        }
        return new TargetDirectory(
                new File( new File( target, "test-data" ), owningTest.getName() ) );
    }

    public static TestDirectory testDirForTest( Class<?> owningTest )
    {
        return forTest( owningTest ).testDirectory();
    }

    public File graphDbDir( boolean clean )
    {
        return directory( "graph-db", clean );
    }

    public void cleanup()
    {
    }

    /*
    public static TargetDirectory forTemporaryFolder( org.junit.rules.TemporaryFolder dir )
    {
        return new TargetDirectory( dir.getRoot() );
    }
    //*/
}
