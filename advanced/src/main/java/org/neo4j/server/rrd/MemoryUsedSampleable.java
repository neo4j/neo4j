/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rrd;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import java.lang.management.ManagementFactory;

public class MemoryUsedSampleable implements Sampleable
{
    private ObjectName memoryName;
    private MBeanServer mbeanServer;

    public MemoryUsedSampleable() throws MalformedObjectNameException
    {
        memoryName = new ObjectName( "java.lang:type=Memory" );
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
    }

    public String getName()
    {
        return "memory_usage_percent";
    }

    public long getValue()
    {
        try
        {
            long used = (Long)( (CompositeDataSupport)mbeanServer.getAttribute( memoryName, "HeapMemoryUsage" ) ).get( "used" );
            long max =  (Long)( (CompositeDataSupport)mbeanServer.getAttribute( memoryName, "HeapMemoryUsage" ) ).get( "max" );
            return Math.round((used / (double)max) * 100);
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
