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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.DelegatingFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class StoreLockerTest
{
    @Inject
    private TestDirectory target;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void shouldUseAlreadyOpenedFileChannel() throws Exception
    {
        StoreChannel channel = mock( StoreChannel.class );
        CustomChannelFileSystemAbstraction fileSystemAbstraction = new CustomChannelFileSystemAbstraction( fileSystem, channel );
        int numberOfCallesToOpen = 0;
        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.storeLayout() ) )
        {
            assertThrows( StoreLockException.class, storeLocker::checkLock );
            numberOfCallesToOpen = fileSystemAbstraction.getNumberOfCallsToOpen();
            // Try to grab lock a second time
            storeLocker.checkLock();
        }
        catch ( StoreLockException e )
        {
            // expected
        }

        assertEquals( numberOfCallesToOpen, fileSystemAbstraction
                .getNumberOfCallsToOpen(), "Expect that number of open channels will remain the same for " );
    }

    @Test
    void shouldAllowMultipleCallsToCheckLock() throws Exception
    {
        try ( StoreLocker storeLocker = new StoreLocker( fileSystem, target.storeLayout() ) )
        {
            storeLocker.checkLock();
            storeLocker.checkLock();
        }
    }

    @Test
    void keepLockWhenOtherTryToTakeLock() throws Exception
    {
        StoreLayout storeLayout = target.storeLayout();
        StoreLocker storeLocker = new StoreLocker( fileSystem, storeLayout );
        storeLocker.checkLock();

        assertThrows( StoreLockException.class, () ->
        {
            try ( StoreLocker storeLocker1 = new StoreLocker( fileSystem, storeLayout ) )
            {
                storeLocker1.checkLock();
            }
        });

        // Initial locker should still have a valid lock
        assertThrows( StoreLockException.class, () ->
        {
            try ( StoreLocker storeLocker1 = new StoreLocker( fileSystem, storeLayout ) )
            {
                storeLocker1.checkLock();
            }
        } );

        storeLocker.close();
    }

    @Test
    void shouldObtainLockWhenStoreFileNotLocked()
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public boolean fileExists( File file )
            {
                return true;
            }
        };

        assertDoesNotThrow( () ->
        {
            try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.storeLayout() ) )
            {
                storeLocker.checkLock();
            }
        } );
    }

    @Test
    void shouldCreateStoreDirAndObtainLockWhenStoreDirDoesNotExist() throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public boolean fileExists( File file )
            {
                return false;
            }
        };

        try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.storeLayout() ) )
        {
            storeLocker.checkLock();
            // Ok
        }
    }

    @Test
    void shouldNotObtainLockWhenStoreDirCannotBeCreated()
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public void mkdirs( File fileName ) throws IOException
            {
                throw new IOException( "store dir could not be created" );
            }

            @Override
            public boolean fileExists( File file )
            {
                return false;
            }
        };

        StoreLayout storeLayout = target.storeLayout();
        StoreLockException storeLockException = assertThrows( StoreLockException.class, () ->
        {
            try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, storeLayout ) )
            {
                storeLocker.checkLock();
            }
        } );
        String msg = format( "Unable to create path for store dir: %s. " + "Please ensure no other process is using this database, and that " +
                "the directory is writable (required even for read-only access)", storeLayout.storeDirectory().toString() );
        assertThat( storeLockException.getMessage(), is( msg ) );
    }

    @Test
    void shouldNotObtainLockWhenUnableToOpenLockFile()
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public StoreChannel write( File fileName ) throws IOException
            {
                throw new IOException( "cannot open lock file" );
            }

            @Override
            public boolean fileExists( File file )
            {
                return false;
            }
        };

        StoreLayout storeLayout = target.storeLayout();
        StoreLockException storeLockException = assertThrows( StoreLockException.class, () ->
        {
            try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, storeLayout ) )
            {
                storeLocker.checkLock();
            }
        } );
        String msg = format( "Unable to obtain lock on store lock file: %s. " +
                        "Please ensure no other process is using this database, and that the " +
                        "directory is writable (required even for read-only access)",
                storeLayout.storeLockFile() );
        assertThat( storeLockException.getMessage(), is( msg ) );
    }

    @Test
    void shouldNotObtainLockWhenStoreAlreadyInUse()
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public boolean fileExists( File file )
            {
                return false;
            }

            @Override
            public StoreChannel write( File fileName ) throws IOException
            {
                return new DelegatingStoreChannel( super.write( fileName ) )
                {
                    @Override
                    public FileLock tryLock()
                    {
                        return null; // 'null' implies that the file has been externally locked
                    }
                };
            }
        };

        StoreLockException storeLockException = assertThrows( StoreLockException.class, () ->
        {
            try ( StoreLocker storeLocker = new StoreLocker( fileSystemAbstraction, target.storeLayout() ) )
            {
                storeLocker.checkLock();
            }
        } );
        assertThat( storeLockException.getMessage(), containsString( "Store and its lock file has been locked by another process" ) );
    }

    @Test
    void mustPreventMultipleInstancesFromStartingOnSameStore()
    {
        File storeDir = target.storeDir();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
        try
        {
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.success();
            }

            assertThrows( Exception.class, () ->
            {
                new TestDatabaseManagementServiceBuilder( storeDir ).build();
            } );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private class CustomChannelFileSystemAbstraction extends DelegatingFileSystemAbstraction
    {
        private final StoreChannel channel;
        private int numberOfCallsToOpen;

        CustomChannelFileSystemAbstraction( FileSystemAbstraction delegate, StoreChannel channel )
        {
            super( delegate );
            this.channel = channel;
        }

        @Override
        public StoreChannel write( File fileName )
        {
            numberOfCallsToOpen++;
            return channel;
        }

        int getNumberOfCallsToOpen()
        {
            return numberOfCallsToOpen;
        }
    }
}
