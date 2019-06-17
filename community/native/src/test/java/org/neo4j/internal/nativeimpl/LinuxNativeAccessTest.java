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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.nativeimpl.NativeAccess.ERROR;

class LinuxNativeAccessTest
{
    private final LinuxNativeAccess nativeAccess = new LinuxNativeAccess();

    @TempDir
    File tempFile;

    @Test
    @EnabledOnOs( OS.LINUX )
    void availableOnLinux()
    {
        assertTrue( nativeAccess.isAvailable() );
    }

    @Test
    @DisabledOnOs( OS.LINUX )
    void disabledOnNonLinux()
    {
        assertFalse( nativeAccess.isAvailable() );
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void failToSkipCacheOnLinuxForIncorrectDescriptor()
    {
        assertEquals( ERROR, nativeAccess.trySkipCache( 0 ) );
        assertEquals( ERROR, nativeAccess.trySkipCache( -1 ) );
    }

    @Test
    @EnabledOnOs( OS.LINUX )
    void skipCacheOnLinuxForCorrectDescriptor() throws IOException, IllegalAccessException
    {
        File file = new File( tempFile, "file" );
        try ( RandomAccessFile randomFile = new RandomAccessFile( file, "rw" ) )
        {
            FileDescriptor fd = randomFile.getFD();
            int descriptor = FieldUtils.getDeclaredField( FileDescriptor.class, "fd", true ).getInt( fd );
            assertEquals( 0, nativeAccess.trySkipCache( descriptor ) );
        }
    }
}
