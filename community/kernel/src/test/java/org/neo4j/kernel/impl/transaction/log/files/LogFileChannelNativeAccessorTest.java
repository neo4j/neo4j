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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestDirectoryExtension
class LogFileChannelNativeAccessorTest
{

    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    private LogFileChannelNativeAccessor channelNativeAccessor;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private StoreChannel testStoreChannel;
    private NativeAccess nativeAccess;

    @BeforeEach
    void setUp() throws IOException
    {
        var filesContext = mock( TransactionLogFilesContext.class );
        nativeAccess = mock( NativeAccess.class, RETURNS_MOCKS );
        when( filesContext.getNativeAccess() ).thenReturn( nativeAccess );
        when( filesContext.getLogProvider() ).thenReturn( logProvider );
        when( filesContext.getRotationThreshold() ).thenReturn( new AtomicLong( 5 ) );

        channelNativeAccessor = new LogFileChannelNativeAccessor( fileSystem, filesContext );
        File originalFile = testDirectory.file( "test" );
        testStoreChannel = fileSystem.write( originalFile );
    }

    @AfterEach
    void tearDown() throws IOException
    {
        if ( testStoreChannel != null )
        {
            testStoreChannel.close();
        }
    }

    @Test
    void adviseSequentialAccess()
    {
        channelNativeAccessor.adviseSequentialAccessAndKeepInCache( testStoreChannel, 0 );

        verify( nativeAccess ).tryAdviseSequentialAccess( anyInt() );
    }

    @Test
    void doNotAdviseSequentialAccessOfClosedChannel() throws IOException
    {
        testStoreChannel.close();
        channelNativeAccessor.adviseSequentialAccessAndKeepInCache( testStoreChannel, 0 );

        verify( nativeAccess, never() ).tryAdviseSequentialAccess( anyInt() );
    }

    @Test
    void evictOpenChannel()
    {
        channelNativeAccessor.evictFromSystemCache( testStoreChannel, 0 );
        verify( nativeAccess ).tryEvictFromCache( anyInt() );
    }

    @Test
    void doNotEvictClosedChannel() throws IOException
    {
        testStoreChannel.close();

        reset( nativeAccess );

        channelNativeAccessor.evictFromSystemCache( testStoreChannel, 0 );
        verify( nativeAccess, never() ).tryEvictFromCache( anyInt() );
    }

    @Test
    void preallocateChannel()
    {
        channelNativeAccessor.preallocateSpace( testStoreChannel, 3 );
        verify( nativeAccess ).tryPreallocateSpace( anyInt(), anyLong() );
    }
}
