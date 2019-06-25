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

import com.sun.jna.Platform;

import static java.lang.Boolean.getBoolean;

public class NativeAccessProvider
{
    private static final boolean DISABLE_NATIVE_ACCESS = getBoolean( NativeAccessProvider.class.getName() + ".disableNativeAccess" );
    private static final AccessHolder HOLDER = new AccessHolder();

    private NativeAccessProvider()
    {
        // no public constructors
    }

    public static NativeAccess getNativeAccess()
    {
        return HOLDER.nativeAccess;
    }
    private static class AccessHolder
    {
        private final NativeAccess nativeAccess;

        AccessHolder()
        {
            if ( DISABLE_NATIVE_ACCESS || !Platform.isLinux() )
            {
                nativeAccess = new AbsentNativeAccess();
            }
            else
            {
                LinuxNativeAccess linuxNativeAccess = new LinuxNativeAccess();
                if ( linuxNativeAccess.isAvailable() )
                {
                    nativeAccess = linuxNativeAccess;
                }
                else
                {
                    nativeAccess = new AbsentNativeAccess();
                }
            }
        }
    }
}
