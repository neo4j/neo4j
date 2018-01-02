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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NotCurrentStoreVersionException;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestIndexProviderStore
{
    private File file;
    private FileSystemAbstraction fileSystem;

    @Before
    public void createStore()
    {
        file = new File( "target/test-data/index-provider-store" );
        fileSystem = new DefaultFileSystemAbstraction();
        file.mkdirs();
        fileSystem.deleteFile( file );
    }

    @Test
    public void lastCommitedTxGetsStoredBetweenSessions() throws Exception
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
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( "3.1" ), true );
        store.close();
        store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( "3.1" ), false );
        store.close();
        try
        {
            new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( "3.5" ), false );
            fail( "Shouldn't be able to upgrade there" );
        }
        catch ( UpgradeNotAllowedByConfigurationException e )
        {   // Good
        }
        store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( "3.5" ), true );
        assertEquals( "3.5", MetaDataStore.versionLongToString( store.getIndexVersion() ) );
        store.close();
    }

    @Test( expected = NotCurrentStoreVersionException.class )
    public void shouldFailToGoBackToOlderVersion() throws Exception
    {
        String newerVersion = "3.5";
        String olderVersion = "3.1";
        try
        {
            IndexProviderStore store = new IndexProviderStore( file, fileSystem,
                    MetaDataStore.versionStringToLong( newerVersion ), true );
            store.close();
            store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( olderVersion ), false );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            assertTrue( e.getMessage().contains( newerVersion ) );
            assertTrue( e.getMessage().contains( olderVersion ) );
            throw e;
        }
    }

    @Test( expected = NotCurrentStoreVersionException.class )
    public void shouldFailToGoBackToOlderVersionEvenIfAllowUpgrade() throws Exception
    {
        String newerVersion = "3.5";
        String olderVersion = "3.1";
        try
        {
            IndexProviderStore store = new IndexProviderStore( file, fileSystem,
                    MetaDataStore.versionStringToLong( newerVersion ), true );
            store.close();
            store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( olderVersion ), true );
        }
        catch ( NotCurrentStoreVersionException e )
        {
            assertTrue( e.getMessage().contains( newerVersion ) );
            assertTrue( e.getMessage().contains( olderVersion ) );
            throw e;
        }
    }

    @Test
    public void upgradeForMissingVersionRecord() throws Exception
    {
        // This was before 1.6.M02
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, 0, false );
        store.close();
        FileUtils.truncateFile( file, 4*8 );
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
        when( tempChannel = fs.open( file, "rw" ) ).then( new Answer<StoreChannel>()
        {
            @Override
            public StoreChannel answer( InvocationOnMock _ ) throws Throwable
            {
                StoreChannel channel = fileSystem.open( file, "rw" );
                if ( channelUsedToCreateFile[0] == null )
                {
                    StoreChannel channelSpy = spy( channel );
                    channelUsedToCreateFile[0] = channelSpy;
                    channel = channelSpy;
                }
                return channel;
            }
        } );

        // Doing the FSA spying above, calling fs.open, actually invokes that method and so a channel
        // is opened. We put that in tempChannel and close it before deleting the file below.
        tempChannel.close();
        fs.deleteFile( file );

        // When
        IndexProviderStore store = new IndexProviderStore( file, fs, MetaDataStore.versionStringToLong( "3.5" ), false );

        // Then
        StoreChannel channel = channelUsedToCreateFile[0];
        verify( channel ).write( any( ByteBuffer.class ), eq( 0L ) );
        verify( channel ).force( true );
        verify( channel ).close();
        verifyNoMoreInteractions( channel );
        store.close();
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowWhenTryingToCreateFileThatAlreadyExists()
    {
        // Given
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        when( fs.fileExists( file ) ).thenReturn( false ).thenReturn( true );
        when( fs.getFileSize( file ) ).thenReturn( 42L );

        // When
        new IndexProviderStore( file, fs, MetaDataStore.versionStringToLong( "3.5" ), false );

        // Then
        // exception is thrown
    }

    @Test
    public void shouldWriteNewFileWhenExistingFileHasZeroLength() throws IOException
    {
        // Given
        file.createNewFile();

        // When
        IndexProviderStore store = new IndexProviderStore( file, fileSystem, MetaDataStore.versionStringToLong( "3.5" ), false );

        // Then
        assertTrue( fileSystem.getFileSize( file ) > 0 );
        store.close();
    }
}
