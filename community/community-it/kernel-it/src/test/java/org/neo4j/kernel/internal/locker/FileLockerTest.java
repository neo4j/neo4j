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
package org.neo4j.kernel.internal.locker;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class FileLockerTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

    static Stream<LockerFactory> lockerFactories()
    {
        return Stream.of( ( fs, directory ) -> new GlobalLocker( fs, directory.storeLayout() ),
                ( fs, directory ) -> new DatabaseLocker( fs, directory.databaseLayout() ) );
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldUseAlreadyOpenedFileChannel( LockerFactory lockerFactory ) throws Exception
    {
        StoreChannel channel = mock( StoreChannel.class );
        CustomChannelFileSystemAbstraction fileSystemAbstraction = new CustomChannelFileSystemAbstraction( fileSystem, channel );
        MutableInt numberOfCallesToOpen = new MutableInt();
        assertThrows( FileLockException.class, () -> {
            try ( Locker locker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
            {
                assertThrows( FileLockException.class, locker::checkLock );
                numberOfCallesToOpen.setValue( fileSystemAbstraction.getNumberOfCallsToOpen() );
                // Try to grab lock a second time
                locker.checkLock();
            }
        } );

        assertEquals( numberOfCallesToOpen.intValue(), fileSystemAbstraction
                .getNumberOfCallsToOpen(), "Expect that number of open channels will remain the same for " );
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldAllowMultipleCallsToCheckLock( LockerFactory lockerFactory ) throws Exception
    {
        try ( Locker locker = lockerFactory.createLocker( fileSystem, testDirectory ) )
        {
            locker.checkLock();
            locker.checkLock();
        }
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void keepLockWhenOtherTryToTakeLock( LockerFactory lockerFactory ) throws Exception
    {
        Locker locker = lockerFactory.createLocker( fileSystem, testDirectory );
        locker.checkLock();

        assertThrows( FileLockException.class, () ->
        {
            try ( Locker locker1 = lockerFactory.createLocker( fileSystem, testDirectory ) )
            {
                locker1.checkLock();
            }
        } );

        // Initial locker should still have a valid lock
        assertThrows( FileLockException.class, () ->
        {
            try ( Locker locker1 = lockerFactory.createLocker( fileSystem, testDirectory ) )
            {
                locker1.checkLock();
            }
        } );

        locker.close();
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldObtainLockWhenFileNotLocked( LockerFactory lockerFactory )
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
            try ( Locker locker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
            {
                locker.checkLock();
            }
        } );
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldCreateDirAndObtainLockWhenDirDoesNotExist( LockerFactory lockerFactory ) throws Exception
    {
        FileSystemAbstraction fileSystemAbstraction = new DelegatingFileSystemAbstraction( fileSystem )
        {
            @Override
            public boolean fileExists( File file )
            {
                return false;
            }
        };

        try ( Locker locker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
        {
            locker.checkLock();
        }
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldNotObtainLockWhenDirCannotBeCreated( LockerFactory lockerFactory )
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

        FileLockException fileLockException = assertThrows( FileLockException.class, () ->
        {
            try ( Locker storeLocker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
            {
                storeLocker.checkLock();
            }
        } );
        assertThat( fileLockException.getMessage(), startsWith( "Unable to create path for dir: " ) );
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldNotObtainLockWhenUnableToOpenLockFile( LockerFactory lockerFactory )
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

        FileLockException fileLockException = assertThrows( FileLockException.class, () ->
        {
            try ( Locker storeLocker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
            {
                storeLocker.checkLock();
            }
        } );
        assertThat( fileLockException.getMessage(), startsWith( "Unable to obtain lock on file:" ) );
    }

    @ParameterizedTest
    @MethodSource( "lockerFactories" )
    void shouldNotObtainLockWhenAlreadyInUse( LockerFactory lockerFactory )
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

        FileLockException fileLockException = assertThrows( FileLockException.class, () ->
        {
            try ( Locker storeLocker = lockerFactory.createLocker( fileSystemAbstraction, testDirectory ) )
            {
                storeLocker.checkLock();
            }
        } );
        assertThat( fileLockException.getMessage(), containsString( "Lock file has been locked by another process" ) );
    }

    @Test
    void mustPreventMultipleInstancesFromStartingOnSameStore()
    {
        File storeDir = testDirectory.storeDir();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDir ).build();
        try
        {
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode();
                tx.commit();
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

    private static class CustomChannelFileSystemAbstraction extends DelegatingFileSystemAbstraction
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

    @FunctionalInterface
    private interface LockerFactory
    {
        Locker createLocker( FileSystemAbstraction fs, TestDirectory directory );
    }
}
