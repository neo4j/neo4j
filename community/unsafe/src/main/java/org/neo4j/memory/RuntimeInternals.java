/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.memory;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.util.FeatureToggles;

import static com.sun.jna.Platform.is64Bit;

/**
 * The ugly part of {@link HeapEstimator} is hidden here.
 */
final class RuntimeInternals
{
    static final boolean DEBUG_ESTIMATIONS = FeatureToggles.flag( HeapEstimator.class, "DEBUG", false );

    static final long LONG_CACHE_MIN_VALUE;
    static final long LONG_CACHE_MAX_VALUE;

    static final int HEADER_SIZE;
    static final int OBJECT_ALIGNMENT;
    static final boolean COMPRESSED_OOPS;

    static final VarHandle STRING_VALUE_ARRAY;

    static
    {
        // Header size
        HEADER_SIZE = guessHeaderSize();

        // Compressed oops and object alignment
        if ( is64Bit() )
        {
            boolean compressedOops;
            int objectAlignment;
            try
            {
                compressedOops = Boolean.parseBoolean( getVmOptionString( "UseCompressedOops" ) );
                objectAlignment = Integer.parseInt( getVmOptionString("ObjectAlignmentInBytes"));
            }
            catch ( Exception e )
            {
                if ( DEBUG_ESTIMATIONS )
                {
                    System.err.println( "HotSpotDiagnostic not available, falling back to guessing. Exception:");
                    e.printStackTrace( System.err );
                }

                // Fallback to guessing
                compressedOops = guessCompressedOops();
                objectAlignment = 8;
            }
            COMPRESSED_OOPS = compressedOops;
            OBJECT_ALIGNMENT = objectAlignment;
        }
        else
        {
            // Values are fixed for 32 bit JVM
            COMPRESSED_OOPS = true;
            OBJECT_ALIGNMENT = 8;
        }

        // get min/max value of cached Long class instances:
        long longCacheMinValue = 0;
        while ( longCacheMinValue > Long.MIN_VALUE && Long.valueOf( longCacheMinValue - 1 ) == Long.valueOf( longCacheMinValue - 1 ) )
        {
            longCacheMinValue -= 1;
        }
        long longCacheMaxValue = -1;
        while ( longCacheMaxValue < Long.MAX_VALUE && Long.valueOf( longCacheMaxValue + 1 ) == Long.valueOf( longCacheMaxValue + 1 ) )
        {
            longCacheMaxValue += 1;
        }
        LONG_CACHE_MIN_VALUE = longCacheMinValue;
        LONG_CACHE_MAX_VALUE = longCacheMaxValue;

        // Compensate for compressed string in Java 9+
        VarHandle stringValueArray;
        try
        {
            stringValueArray = MethodHandles.privateLookupIn( String.class, MethodHandles.lookup() ).findVarHandle( String.class, "value", byte[].class );
        }
        catch ( NoSuchFieldException | IllegalAccessException e )
        {
            stringValueArray = null;
        }
        STRING_VALUE_ARRAY = stringValueArray;
    }

    private RuntimeInternals()
    {
    }

    static int stringBackingArraySize( String s )
    {
        if ( STRING_VALUE_ARRAY != null )
        {
            byte[] value = (byte[]) STRING_VALUE_ARRAY.get( s );
            return value.length;
        }
        return s.length() << 1; // Assume UTF16
    }

    private static String getVmOptionString( String key ) throws Exception
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        ObjectName mbean = new ObjectName( "com.sun.management:type=HotSpotDiagnostic" );
        CompositeDataSupport val = (CompositeDataSupport) server.invoke( mbean, "getVMOption", new Object[]{key}, new String[]{"java.lang.String"} );
        return val.get( "value" ).toString();
    }

    @SuppressWarnings( "unused" )
    public static class CompressedOopsClass
    {
        public Object obj1;
        public Object obj2;
    }

    @SuppressWarnings( "unused" )
    public static class HeaderClass
    {
        public boolean b1;
    }

    private static boolean guessCompressedOops()
    {
        long off1 = UnsafeUtil.getFieldOffset( CompressedOopsClass.class, "obj1" );
        long off2 = UnsafeUtil.getFieldOffset( CompressedOopsClass.class, "obj2" );
        return Math.abs( off2 - off1 ) == 4;
    }

    private static int guessHeaderSize()
    {
        long off1 = UnsafeUtil.getFieldOffset( HeaderClass.class, "b1" );
        return (int) off1;
    }
}
