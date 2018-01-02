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
package org.neo4j.server.rrd.sampler;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;

import org.neo4j.jmx.JmxUtils;
import org.neo4j.server.rrd.Sampleable;
import org.rrd4j.DsType;

public class MemoryUsedSampleable implements Sampleable
{

    private final ObjectName memoryName;

    public MemoryUsedSampleable()
    {
        try
        {
            memoryName = new ObjectName( "java.lang:type=Memory" );
        } catch ( MalformedObjectNameException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public String getName()
    {
        return "memory_usage_percent";
    }

    @Override
    public double getValue()
    {
        CompositeDataSupport heapMemoryUsage = JmxUtils.getAttribute( memoryName, "HeapMemoryUsage" );
        long used = (Long) heapMemoryUsage.get( "used" );
        long max = (Long) heapMemoryUsage.get( "max" );
        return Math.ceil( 100.0 * used / max );
    }

    @Override
    public DsType getType()
    {
        return DsType.GAUGE;
    }
}
