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
package org.neo4j.kernel.impl.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

/**
 * Utility class that exposes methods from proprietary implementations of {@link OperatingSystemMXBean}.
 * Able to work on Oracle JDK and IBM JDK.
 * Methods never fail but instead return {@link #VALUE_UNAVAILABLE} if such method is not exposed by the underlying
 * MX bean.
 */
public final class OsBeanUtil
{
    public static final long VALUE_UNAVAILABLE = -1;

    private static final String SUN_OS_BEAN = "com.sun.management.OperatingSystemMXBean";
    private static final String SUN_UNIX_OS_BEAN = "com.sun.management.UnixOperatingSystemMXBean";
    private static final String IBM_OS_BEAN = "com.ibm.lang.management.OperatingSystemMXBean";

    private static final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();

    private static final Method getTotalPhysicalMemoryMethod;
    private static final Method getFreePhysicalMemoryMethod;
    private static final Method getCommittedVirtualMemoryMethod;
    private static final Method getTotalSwapSpaceMethod;
    private static final Method getFreeSwapSpaceMethod;
    private static final Method getMaxFileDescriptorsMethod;
    private static final Method getOpenFileDescriptorsMethod;

    static
    {
        getTotalPhysicalMemoryMethod = findOsBeanMethod( "getTotalPhysicalMemorySize", "getTotalPhysicalMemory" );
        getFreePhysicalMemoryMethod = findOsBeanMethod( "getFreePhysicalMemorySize", "getFreePhysicalMemorySize" );
        getCommittedVirtualMemoryMethod = findOsBeanMethod( "getCommittedVirtualMemorySize", null );
        getTotalSwapSpaceMethod = findOsBeanMethod( "getTotalSwapSpaceSize", "getTotalSwapSpaceSize" );
        getFreeSwapSpaceMethod = findOsBeanMethod( "getFreeSwapSpaceSize", "getFreeSwapSpaceSize" );
        getMaxFileDescriptorsMethod = findUnixOsBeanMethod( "getMaxFileDescriptorCount" );
        getOpenFileDescriptorsMethod = findUnixOsBeanMethod( "getOpenFileDescriptorCount" );
    }

    private OsBeanUtil()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    /**
     * @return total amount of physical memory in bytes, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getTotalPhysicalMemory()
    {
        return invoke( getTotalPhysicalMemoryMethod );
    }

    /**
     * @return amount of free physical memory in bytes, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getFreePhysicalMemory()
    {
        return invoke( getFreePhysicalMemoryMethod );
    }

    /**
     * @return amount of virtual memory that is guaranteed to be available to the running process in bytes, or
     * {@link #VALUE_UNAVAILABLE} if underlying bean does not provide this functionality.
     */
    public static long getCommittedVirtualMemory()
    {
        return invoke( getCommittedVirtualMemoryMethod );
    }

    /**
     * @return total amount of swap space in bytes, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getTotalSwapSpace()
    {
        return invoke( getTotalSwapSpaceMethod );
    }

    /**
     * @return total amount of free swap space in bytes, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getFreeSwapSpace()
    {
        return invoke( getFreeSwapSpaceMethod );
    }

    /**
     * @return maximum number of file descriptors, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getMaxFileDescriptors()
    {
        return invoke( getMaxFileDescriptorsMethod );
    }

    /**
     * @return number of open file descriptors, or {@link #VALUE_UNAVAILABLE} if underlying bean does not
     * provide this functionality.
     */
    public static long getOpenFileDescriptors()
    {
        return invoke( getOpenFileDescriptorsMethod );
    }

    private static Method findOsBeanMethod( String sunMethodName, String ibmMethodName )
    {
        Method sunOsBeanMethod = findSunOsBeanMethod( sunMethodName );
        return sunOsBeanMethod == null ? findIbmOsBeanMethod( ibmMethodName ) : sunOsBeanMethod;
    }

    private static Method findUnixOsBeanMethod( String methodName )
    {
        return findMethod( SUN_UNIX_OS_BEAN, methodName );
    }

    private static Method findSunOsBeanMethod( String methodName )
    {
        return findMethod( SUN_OS_BEAN, methodName );
    }

    private static Method findIbmOsBeanMethod( String methodName )
    {
        return findMethod( IBM_OS_BEAN, methodName );
    }

    private static Method findMethod( String className, String methodName )
    {
        try
        {
            return (methodName == null) ? null : Class.forName( className ).getMethod( methodName );
        }
        catch ( Throwable t )
        {
            return null;
        }
    }

    private static long invoke( Method method )
    {
        try
        {
            Object value = (method == null) ? null : method.invoke( osBean );
            return (value == null) ? VALUE_UNAVAILABLE : ((Number) value).longValue();
        }
        catch ( Throwable t )
        {
            return VALUE_UNAVAILABLE;
        }
    }
}
