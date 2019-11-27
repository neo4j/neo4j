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
package org.neo4j.internal.nativeimpl;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileStore;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.nativeimpl.NativeAccess.ERROR;

class LinuxNativeAccessTest
{
    private final LinuxNativeAccess nativeAccess = new LinuxNativeAccess();

    @Test
    @DisabledOnOs( OS.LINUX )
    void disabledOnNonLinux()
    {
        assertFalse( nativeAccess.isAvailable() );
    }

    @Nested
    @EnabledOnOs( OS.LINUX )
    class AccessLinuxMethodsTest
    {
        @TempDir
        File tempFile;

        @Test
        void availableOnLinux()
        {
            assertTrue( nativeAccess.isAvailable() );
        }

        @Test
        void accessErrorMessageOnError() throws IOException, IllegalAccessException
        {
            File file = new File( tempFile, "file" );
            int descriptor = getClosedDescriptor( file );
            var nativeCallResult = nativeAccess.tryPreallocateSpace( descriptor, 1024 );
            assertNotEquals( 0, nativeCallResult.getErrorCode() );
            assertThat( nativeCallResult.getErrorMessage() ).isNotEmpty();
        }

        @Test
        void failToPreallocateOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            var preallocateResult = nativeAccess.tryPreallocateSpace( 0, 1024 );
            assertEquals( ERROR, preallocateResult.getErrorCode() );
            assertTrue( preallocateResult.isError() );

            var negativeDescriptor = nativeAccess.tryPreallocateSpace( -1, 1024 );
            assertEquals( ERROR, negativeDescriptor.getErrorCode() );
            assertTrue( negativeDescriptor.isError() );

            File file = new File( tempFile, "file" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryPreallocateSpace( descriptor, 1024 ) );
        }

        @Test
        void preallocateCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            FileStore fileStore = Files.getFileStore( tempFile.toPath() );
            long blockSize = fileStore.getBlockSize();
            File file = new File( tempFile, "preallocated1" );
            File file2 = new File( tempFile, "preallocated2" );
            File file3 = new File( tempFile, "preallocated3" );
            long size1 = blockSize - 1;
            long size2 = blockSize;
            long size3 = 2 * blockSize;

            preallocate( file, size1 );
            preallocate( file2, size2 );
            preallocate( file3, size3 );

            assertEquals( size1, file.length() );
            assertEquals( size2, file2.length() );
            assertEquals( size3, file3.length() );
        }

        @Test
        void failToAdviseSequentialOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            var nativeCallResult = nativeAccess.tryAdviseSequentialAccess( 0 );
            assertEquals( ERROR, nativeCallResult.getErrorCode() );
            assertTrue( nativeCallResult.isError() );

            var negativeDescriptorResult = nativeAccess.tryAdviseSequentialAccess( -1 );
            assertEquals( ERROR, negativeDescriptorResult.getErrorCode() );
            assertTrue( negativeDescriptorResult.isError() );

            File file = new File( tempFile, "sequentialFile" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryAdviseSequentialAccess( descriptor ) );
        }

        @Test
        void adviseSequentialAccessOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            File file = new File( tempFile, "correctSequentialFile" );
            try ( RandomAccessFile randomFile = new RandomAccessFile( file, "rw" ) )
            {
                int descriptor = getDescriptor( randomFile );
                var nativeCallResult = nativeAccess.tryAdviseSequentialAccess( descriptor );
                assertEquals( 0, nativeCallResult.getErrorCode() );
                assertFalse( nativeCallResult.isError() );
            }
        }

        @Test
        void failToSkipCacheOnLinuxForIncorrectDescriptor() throws IOException, IllegalAccessException
        {
            assertEquals( ERROR, nativeAccess.tryEvictFromCache( 0 ).getErrorCode() );
            assertEquals( ERROR, nativeAccess.tryEvictFromCache( -1 ).getErrorCode() );

            File file = new File( tempFile, "file" );
            int descriptor = getClosedDescriptor( file );
            assertNotEquals( 0, nativeAccess.tryEvictFromCache( descriptor ) );
        }

        @Test
        void skipCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
        {
            File file = new File( tempFile, "file" );
            try ( RandomAccessFile randomFile = new RandomAccessFile( file, "rw" ) )
            {
                int descriptor = getDescriptor( randomFile );
                assertFalse( nativeAccess.tryEvictFromCache( descriptor ).isError() );
            }
        }
    }

    private void preallocate( File file, long bytes ) throws IOException, IllegalAccessException
    {
        try ( RandomAccessFile randomFile = new RandomAccessFile( file, "rw" ) )
        {
            int descriptor = getDescriptor( randomFile );
            assertFalse( nativeAccess.tryPreallocateSpace( descriptor, bytes ).isError() );
        }
    }

    private int getClosedDescriptor( File file ) throws IOException, IllegalAccessException
    {
        try ( RandomAccessFile randomFile = new RandomAccessFile( file, "rw" ) )
        {
            return getDescriptor( randomFile );
        }
    }

    private int getDescriptor( RandomAccessFile randomFile ) throws IOException, IllegalAccessException
    {
        FileDescriptor fd = randomFile.getFD();
        return FieldUtils.getDeclaredField( FileDescriptor.class, "fd", true ).getInt( fd );
    }
}
