/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.Map;

public class AutoConfigurator
{
    private final int totalPhysicalMemMb;
    private final int maxVmUsageMb;
    private final String dbPath;
    private final boolean useMemoryMapped;

    public AutoConfigurator( String dbPath, boolean useMemoryMapped, boolean dump )
    {
        this.dbPath = dbPath;
        this.useMemoryMapped = useMemoryMapped;
        OperatingSystemMXBean osBean =
            ManagementFactory.getOperatingSystemMXBean();
        long mem = -1;
        try
        {
            Class<?> beanClass =
                Class.forName( "com.sun.management.OperatingSystemMXBean" );
            Method method = beanClass.getMethod( "getTotalPhysicalMemorySize" );
            mem = (Long) method.invoke( osBean );
        }
        catch ( Exception e )
        { // ok we tried but probably 1.5 JVM or other class library implementation
        }
        catch ( LinkageError e )
        { // ok we tried but probably 1.5 JVM or other class library implementation
        }
        if ( mem != -1 )
        {
            totalPhysicalMemMb = (int) (mem / 1024 / 1024 );
        }
        else
        {
            totalPhysicalMemMb = -1;
        }
        mem = Runtime.getRuntime().maxMemory();
        maxVmUsageMb = (int) ( mem / 1024 / 1024 );
        if ( dump )
        {
            System.out.println( getNiceMemoryInformation() );
        }
    }

    public String getNiceMemoryInformation()
    {
        return "Physical mem: " + totalPhysicalMemMb + "MB, Heap size: " + maxVmUsageMb + "MB";
    }

    public void configure( Map<Object,Object> config )
    {
        if ( totalPhysicalMemMb > 0 )
        {
            if ( useMemoryMapped )
            {
                int availableMem = (totalPhysicalMemMb - maxVmUsageMb );
                // leave 15% for OS and other progs
                availableMem -= (int) ( availableMem * 0.15f );
                assignMemory( config, availableMem );
            }
            else
            {
                // use half of heap (if needed) for buffers
                assignMemory( config, maxVmUsageMb / 2 );
            }
        }
    }

    private int calculate( int memLeft, int storeSize, float use, float expand,
            boolean canExpand )
    {
        int size = memLeft;
        if ( storeSize > (memLeft * use) )
        {
            size = (int) (memLeft * use);
        }
        else if ( canExpand  )
        {
            if ( (storeSize * expand * 5 <  memLeft * use ) )
            {
                size = (int) (memLeft * use / 5);
            }
            else
            {
                size = (int) (memLeft * use);
            }
        }
        else
        {
            size = storeSize;
        }
        return size;
    }

    private void assignMemory( Map<Object, Object> config, int availableMem )
    {
        int nodeStore = getFileSizeMb( "nodestore.db" );
        int relStore = getFileSizeMb( "relationshipstore.db" );
        int propStore = getFileSizeMb( "propertystore.db" );
        int stringStore = getFileSizeMb( "propertystore.db.strings" );
        int arrayStore = getFileSizeMb( "propertyStore.db.arrays" );

        int totalSize =
            nodeStore + relStore + propStore + stringStore + arrayStore;
        boolean expand = false;
        if ( totalSize * 1.15f < availableMem )
        {
            expand = true;
        }
        int memLeft = availableMem;
        relStore = calculate( memLeft, relStore, 0.75f, 1.1f, expand );
        memLeft -= relStore;
        nodeStore = calculate( memLeft, nodeStore, 0.2f, 1.1f, expand );
        memLeft -= nodeStore;
        propStore = calculate( memLeft, propStore, 0.75f, 1.1f, expand );
        memLeft -= propStore;
        stringStore = calculate( memLeft, stringStore, 0.75f, 1.1f, expand );
        memLeft -= stringStore;
        arrayStore = calculate( memLeft, arrayStore, 1.0f, 1.1f, expand );
        memLeft -= arrayStore;

        configPut( config, "nodestore.db", nodeStore );
        configPut( config, "relationshipstore.db", relStore );
        configPut( config, "propertystore.db", propStore );
        configPut( config, "propertystore.db.strings", stringStore );
        configPut( config, "propertystore.db.arrays", arrayStore );
    }

    private void configPut( Map<Object, Object> config, String store,
            int size )
    {
        config.put( "neostore." + store + ".mapped_memory", size + "M" );
    }

    private int getFileSizeMb( String file )
    {
        long length = new File( dbPath + File.separator + "neostore." + file ).length();
        int mb = (int) ( length / 1024 / 1024 );
        if ( mb > 0 )
        {
            return mb;
        }
        // default return 1MB if small or empty file
        return 1;
    }
}