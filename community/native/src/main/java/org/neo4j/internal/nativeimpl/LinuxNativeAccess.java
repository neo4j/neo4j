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

import com.sun.jna.Native;
import com.sun.jna.Platform;

import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

public class LinuxNativeAccess implements NativeAccess
{
    /**
     * Constant defined in fadvise.h and means that suggest that the specified data will not be accessed in the near future.
     * For more info check man page for posix_fadvise.
     */
    private static final int POSIX_FADV_DONTNEED = 4;

    private static boolean nativeAccessAvailable;
    private static Throwable initializationFailure;

    static
    {
        try
        {
            if ( Platform.isLinux() )
            {
                Native.register( Platform.C_LIBRARY_NAME );
                nativeAccessAvailable = true;
            }
            else
            {
                nativeAccessAvailable = false;
            }
        }
        catch ( Throwable t )
        {
            initializationFailure = t;
            nativeAccessAvailable = false;
        }
    }

    /**
     * Declare an access pattern for file data. Announce an intention to access file data in a specific pattern in the future,
     * thus allowing the kernel to perform appropriate optimizations.
     * The advice applies to a region starting at offset and extending for len bytes (or until the end of the file if len is 0)
     * within the file referred to by fd. The advice is not binding; it merely constitutes an expectation on behalf of the application.
     * @param fd file descriptor
     * @param offset offset in the file
     * @param len advise len in bytes
     * @param flag advise options
     * @return 0 on success. On error, an error number is returned
     */
    private static native int posix_fadvise( int fd, long offset, long len, int flag );

    @Override
    public boolean isAvailable()
    {
        return nativeAccessAvailable;
    }

    @Override
    public int trySkipCache( int fd )
    {
        if ( fd <= 0 )
        {
            return NativeAccess.ERROR;
        }
        return posix_fadvise( fd, 0, 0, POSIX_FADV_DONTNEED );
    }

    @Override
    public String describe()
    {
        if ( nativeAccessAvailable )
        {
            return "Linux native access is available.";
        }
        StringBuilder descriptionBuilder = new StringBuilder( "Linux native access is not available." );
        if ( initializationFailure != null )
        {
            String exception = getStackTrace( initializationFailure );
            descriptionBuilder.append( " Details: " ).append( exception );
        }
        return descriptionBuilder.toString();
    }
}
