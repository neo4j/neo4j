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
package org.neo4j.kernel.internal;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.internal.locker.StoreLocker.STORE_LOCK_FILENAME;

public class StoreLockerTest
{
    @Rule
    public final TestDirectory target = TestDirectory.testDirectory();
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void shouldUseAlreadyOpenedFileChannel() throws Exception
    {
        StoreChannel channel = Mockito.mock( StoreChannel.class );
        CustomChannelFileSystemAbstraction fileSystemAbstraction =
                new CustomChannelFileSystemAbstraction( fileSystemRule.get(), channel );
        int numberOfCallesToOpen = 0;
        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.directory( "unused" ) ) )
        {
            try
            {
                storeLocker.checkLock();
                fail();
            }
            catch ( StoreLockException e )
            {
                numberOfCallesToOpen = fileSystemAbstraction.getNumberOfCallsToOpen();

                // Try to grab lock a second time
                storeLocker.checkLock();
            }
        }
        catch ( StoreLockException e )
        {
            // expected
        }

        assertEquals( "Expect that number of open channels will remain the same for ",
                numberOfCallesToOpen, fileSystemAbstraction
                .getNumberOfCallsToOpen() );
    }

    @Test
    public void shouldAllowMultipleCallsToCheckLock() throws Exception
    {
        try ( StoreLocker storeLocker = new StoreLocker( fileSystemRule.get(), target.directory( "unused" ) ) )
        {
            storeLocker.checkLock();
            storeLocker.checkLock();
        }
    }

    @Test
    public void keepLockWhenOtherTryToTakeLock() throws Exception
    {
        File directory = target.directory( "unused" );
        DefaultFileSystemAbstraction fileSystemAbstraction = fileSystemRule.get();
        StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, directory );
        storeLocker.checkLock();

        try ( StoreLocker storeLocker1 = new StoreLocker( fileSystemAbstraction, directory ) )
        {
            storeLocker1.checkLock();
            fail();
        }
        catch ( StoreLockException e )
        {
            // Expected
        }

        // Initial locker should still have a valid lock
        try ( StoreLocker storeLocker1 = new StoreLocker( fileSystemAbstraction, directory ) )
        {
            storeLocker1.checkLock();
            fail();
        }
        catch ( StoreLockException e )
        {
            // Expected
        }

        storeLocker.close();
    }

    @Test
    public void shouldObtainLockWhenStoreFileNotLocked() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystemRule.get() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return true;
            }
        };

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.directory( "unused" ) ) )
        {
            storeLocker.checkLock();

            // Ok
        }
        catch ( StoreLockException e )
        {
            fail();
        }
    }

    @Test
    public void shouldCreateStoreDirAndObtainLockWhenStoreDirDoesNotExist() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystemRule.get() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }
        };

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.directory( "unused" ) ) )
        {
            storeLocker.checkLock();
            // Ok
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreDirCannotBeCreated() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystemRule.get() )
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

        File storeDir = target.directory( "unused" );

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, storeDir ) )
        {
            storeLocker.checkLock();
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to create path for store dir: %s. " +
                    "Please ensure no other process is using this database, and that " +
                    "the directory is writable (required even for read-only access)", storeDir );
            assertThat( e.getMessage(), is( msg ) );
        }
    }

    @Test
    public void shouldNotObtainLockWhenUnableToOpenLockFile() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystemRule.get() )
        {
            @Override
            public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
            {
                throw new IOException( "cannot open lock file" );
            }

            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }
        };

        File storeDir = target.directory( "unused" );

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, storeDir ) )
        {
            storeLocker.checkLock();
            fail();
        }
        catch ( StoreLockException e )
        {
            String msg = format( "Unable to obtain lock on store lock file: %s. " +
                            "Please ensure no other process is using this database, and that the " +
                            "directory is writable (required even for read-only access)",
                    new File( storeDir, STORE_LOCK_FILENAME ) );
            assertThat( e.getMessage(), is( msg ) );
        }
    }

    @Test
    public void shouldNotObtainLockWhenStoreAlreadyInUse() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystemRule.get() )
        {
            @Override
            public boolean fileExists( File fileName )
            {
                return false;
            }

            @Override
            public StoreChannel open( File fileName, OpenMode openMode ) throws IOException
            {
                return new DelegatingStoreChannel( super.open( fileName, openMode ) )
                {
                    @Override
                    public FileLock tryLock()
                    {
                        return null; // 'null' implies that the file has been externally locked
                    }
                };
            }
        };

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.directory( "unused" ) ) )
        {
            storeLocker.checkLock();
            fail();
        }
        catch ( StoreLockException e )
        {
            assertThat( e.getMessage(),
                    containsString( "Store and its lock file has been locked by another process" ) );
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

    private class CustomChannelFileSystemAbstraction extends DelegatingFileSystemAbstraction
    {
        private final StoreChannel channel;
        private int numberOfCallsToOpen;

        CustomChannelFileSystemAbstraction( DefaultFileSystemAbstraction delegate, StoreChannel channel )
        {
            super( delegate );
            this.channel = channel;
        }

        @Override
        public StoreChannel open( File fileName, OpenMode openMode )
        {
            numberOfCallsToOpen++;
            return channel;
        }

        public int getNumberOfCallsToOpen()
        {
            return numberOfCallsToOpen;
        }
    }
}
