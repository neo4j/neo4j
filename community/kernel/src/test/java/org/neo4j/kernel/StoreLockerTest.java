/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.StoreLocker.STORE_LOCK_FILENAME;

public class StoreLockerTest
{
    @Rule
    public TargetDirectory.TestDirectory target = TargetDirectory.testDirForTest( StoreLockerTest.class );

    @Test
    public void shouldObtainLockWhenStoreFileNotLocked() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( new DefaultFileSystemAbstraction() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return true;
            }
        };
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.directory( "unused" ) );

            // Ok
        }
        catch ( StoreLockException e )
        {
            fail();
        }
        finally
        {
            storeLocker.release();
        }
    }

    @Test
    public void shouldCreateStoreDirAndObtainLockWhenStoreDirDoesNotExist() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( new DefaultFileSystemAbstraction() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }
        };
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.directory( "unused" ) );
            // Ok
        }
        finally
        {
            storeLocker.release();
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreDirCannotBeCreated() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( new DefaultFileSystemAbstraction() )
        {
            @Override
            public void mkdirs( File fileName ) throws IOException
            {
                throw new IOException( "store dir could not be created" );
            }

            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }
        };
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );
        File storeDir = target.directory( "unused" );

        try
        {
            storeLocker.checkLock( storeDir );
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to create path for store dir: %s. Please ensure no other process is using this database, and that the directory is writable (required even for read-only access)", storeDir );
            assertThat( e.getMessage(), is( msg ) );
        }
        finally
        {
            storeLocker.release();
        }
    }

    @Test
    public void shouldNotObtainLockWhenUnableToOpenLockFile() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( new DefaultFileSystemAbstraction() )
        {
            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                throw new IOException( "cannot open lock file" );
            }

            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }
        };
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );
        File storeDir = target.directory( "unused" );

        try
        {
            storeLocker.checkLock( storeDir );
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to obtain lock on store lock file: %s. Please ensure no other process is using this database, and that the directory is writable (required even for read-only access)", new File( storeDir,
                    STORE_LOCK_FILENAME ) );
            assertThat( e.getMessage(), is( msg ) );
        }
        finally
        {
            storeLocker.release();
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreAlreadyInUse() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( new DefaultFileSystemAbstraction() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }

            @Override
            public StoreChannel open( File fileName, String mode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, mode ) )
                {
                    @Override
                    public FileLock tryLock() throws IOException
                    {
                        return null; // 'null' implies that the file has been externally locked
                    }
                };
            }
        };
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction );

        try
        {
            storeLocker.checkLock( target.directory( "unused" ) );
            fail();
        }
        catch ( StoreLockException e )
        {
            assertThat( e.getMessage(), containsString( "Store and its lock file has been locked by another process" ) );
        }
        finally
        {
            storeLocker.release();
        }
    }

    @Test
    public void mustPreventMultipleInstancesFromStartingOnSameStore()
    {
        File storeDir = target.graphDbDir();
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        try
        {
            new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
            fail( "Should not be able to start up another db in the same dir" );
        }
        catch ( Exception e )
        {
            // Good
        }
        finally
        {
            db.shutdown();
        }
    }
}
