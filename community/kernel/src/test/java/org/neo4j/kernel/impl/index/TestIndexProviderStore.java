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
package org.neo4j.kernel.impl.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.Resource;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.io.fs.FileUtils.truncateFile;
import static org.neo4j.io.fs.OpenMode.READ_WRITE;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionLongToString;
import static org.neo4j.kernel.impl.store.MetaDataStore.versionStringToLong;

@ExtendWith( TestDirectoryExtension.class )
public class TestIndexProviderStore
{
    @Resource
    public TestDirectory testDirectory;
    private File file;
    private FileSystemAbstraction fileSystem;

    @BeforeEach
    public void createStore()
    {
        file = testDirectory.file( "index-provider-store" );
        fileSystem = new DefaultFileSystemAbstraction();
        file.mkdirs();
        fileSystem.deleteFile( file );
    }

    @AfterEach
    public void tearDown() throws IOException
    {
        fileSystem.close();
    }

    @Test
    public void lastCommitedTxGetsStoredBetweenSessions()
    {
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, 0, false );
        store.setVersion( 5 );
        store.setLastCommittedTx( 12 );
        store.close();
        store = new IndexProviderStore( file, fileSystem, 0, false );
        assertEquals( 5, store.getVersion() );
        assertEquals( 12, store.getLastCommittedTx() );
        store.close();
    }

    @Test
    public void shouldFailUpgradeIfNotAllowed()
    {
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.1" ), true );
        store.close();
        store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.1" ), false );
        store.close();
        try
        {
            new IndexProviderStore( file, fileSystem, versionStringToLong( "3.5" ), false );
            fail( "Shouldn't be able to upgrade there" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {   // Good
        }
        store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.5" ), true );
        assertEquals( "3.5", versionLongToString( store.getIndexVersion() ) );
        store.close();
    }

    @Test
    public void shouldFailToGoBackToOlderVersion()
    {
        assertThrows( NotCurrentStoreVersionException.class, () -> {
            String newerVersion = "3.5";
            String olderVersion = "3.1";
            try
            {
                IndexProviderStore store =
                        new IndexProviderStore( file, fileSystem, versionStringToLong( newerVersion ), true );
                store.close();
                store = new IndexProviderStore( file, fileSystem, versionStringToLong( olderVersion ), false );
            }
            catch ( NotCurrentStoreVersionException e )
            {
                assertTrue( e.getMessage().contains( newerVersion ) );
                assertTrue( e.getMessage().contains( olderVersion ) );
                throw e;
            }
        } );
    }

    @Test
    public void shouldFailToGoBackToOlderVersionEvenIfAllowUpgrade()
    {
        assertThrows( NotCurrentStoreVersionException.class, () -> {
            String newerVersion = "3.5";
            String olderVersion = "3.1";
            try
            {
                IndexProviderStore store =
                        new IndexProviderStore( file, fileSystem, versionStringToLong( newerVersion ), true );
                store.close();
                store = new IndexProviderStore( file, fileSystem, versionStringToLong( olderVersion ), true );
            }
            catch ( NotCurrentStoreVersionException e )
            {
                assertTrue( e.getMessage().contains( newerVersion ) );
                assertTrue( e.getMessage().contains( olderVersion ) );
                throw e;
            }
        } );
    }

    @Test
    public void upgradeForMissingVersionRecord() throws Exception
    {
        // This was before 1.6.M02
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, 0, false );
        store.close();
        truncateFile( file, 4 * 8 );
        try
        {
            store = new IndexProviderStore( file, fileSystem, 0, false );
            fail( "Should have thrown upgrade exception" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {   // Good
        }

        store = new IndexProviderStore( file, fileSystem, 0, true );
        store.close();
    }

    @Test
    public void shouldForceChannelAfterWritingMetadata() throws IOException
    {
        // Given
        final StoreChannel[] channelUsedToCreateFile = {null};

        FileSystemAbstraction fs = spy( fileSystem );
        StoreChannel tempChannel;
        when( tempChannel = fs.open( file, READ_WRITE ) ).then( ignored ->
        {
            StoreChannel channel = fileSystem.open( file, READ_WRITE );
            if ( channelUsedToCreateFile[0] == null )
            {
                StoreChannel channelSpy = spy( channel );
                channelUsedToCreateFile[0] = channelSpy;
                channel = channelSpy;
            }
            return channel;
        } );

        // Doing the FSA spying above, calling fs.open, actually invokes that method and so a channel
        // is opened. We put that in tempChannel and close it before deleting the file below.
        tempChannel.close();
        fs.deleteFile( file );

        // When
        IndexProviderStore store = new IndexProviderStore( file, fs, versionStringToLong( "3.5" ), false );

        // Then
        StoreChannel channel = channelUsedToCreateFile[0];
        verify( channel ).writeAll( any( ByteBuffer.class ), eq( 0L ) );
        verify( channel ).force( true );
        verify( channel ).close();
        verifyNoMoreInteractions( channel );
        store.close();
    }

    @Test
    public void shouldThrowWhenTryingToCreateFileThatAlreadyExists()
    {
        assertThrows( IllegalArgumentException.class, () -> {
            // Given
            FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
            when( fs.fileExists( file ) ).thenReturn( false ).thenReturn( true );
            when( fs.getFileSize( file ) ).thenReturn( 42L );

            // When
            new IndexProviderStore( file, fs, versionStringToLong( "3.5" ), false );

            // Then
            // exception is thrown
        } );
    }

    @Test
    public void shouldWriteNewFileWhenExistingFileHasZeroLength() throws IOException
    {
        // Given
        file.createNewFile();

        // When
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, versionStringToLong( "3.5" ), false );

        // Then
        assertTrue( fileSystem.getFileSize( file ) > 0 );
        store.close();
    }
}
