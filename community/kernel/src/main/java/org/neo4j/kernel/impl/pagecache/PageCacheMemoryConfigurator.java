/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.pagecache;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import org.neo4j.function.Function2;

/** Configures amount of memory to use for the cache, if the user has not hard-coded a value. */
public class PageCacheMemoryConfigurator implements Function2<Long, String, Long>
{
    private final long freeMemory;
    private final long jvmMemory;

    public static PageCacheMemoryConfigurator fallbackToAutoConfig()
    {
        return null;
    }

    private static long freePhysicalMemory()
    {
        OperatingSystemMXBean osBean =
                ManagementFactory.getOperatingSystemMXBean();
        long mem;
        try
        {
            Class<?> beanClass =
                    Thread.currentThread().getContextClassLoader()
                            .loadClass( "com.sun.management.OperatingSystemMXBean" );
            Method method = beanClass.getMethod( "getFreePhysicalMemorySize" );
            mem = (Long) method.invoke( osBean );
        }
        catch ( Exception e )
        {
            // ok we tried but probably 1.5 JVM or other class library implementation
            mem = -1; // Be explicit about how this error is handled.
        }
        catch ( LinkageError e )
        {
            // ok we tried but probably 1.5 JVM or other class library implementation
            mem = -1; // Be explicit about how this error is handled.
        }
        return mem;
    }

    public PageCacheMemoryConfigurator( long freeMemory, long jvmMemory )
    {
        this.freeMemory = freeMemory;
        this.jvmMemory = jvmMemory;
    }

    @Override
    public Long apply( Long value, String rawValue )
    {
        return null;
    }
}
