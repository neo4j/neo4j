/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.desktop.config;

import java.lang.management.ManagementFactory;

/**
 * Value is in Mb
 */
public class HeapSizeConfig extends OsSpecific<Value<Integer>>
{
    public static final long KILO = 1024;
    public static final long MEGA = KILO*KILO;
    public static final long GIGA = MEGA*KILO;
    
    private final Environment environment;

    public HeapSizeConfig( Environment environment )
    {
        this.environment = environment;
    }
    
    @Override
    protected Value<Integer> getFor( Os os )
    {
        switch ( os )
        {
        case WINDOWS: return new Launch4jIniValue( environment );
        default: throw new UnsupportedOperationException( os.name() );
        }
    }
    
    @SuppressWarnings( "restriction" )
    public static int getAvailableTotalPhysicalMemoryMb()
    {
        long memorySize = ((com.sun.management.OperatingSystemMXBean)
                ManagementFactory.getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
        return (int) (memorySize / MEGA);
    }
}
