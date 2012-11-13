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
package org.neo4j.kernel.impl.core;

import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.nioneo.store.PropertyDatas;

public class TestBinarySearchPerformance
{
    @Ignore( "Not a unit test, enable to try out performance difference" )
    @Test
    public void measurePerformance() throws Exception
    {
        for ( int i = 1; i < 100; i++ )
        {
            System.out.println( "===" + i + "===" );
            test( i );
        }
    }
    
    private void test( int size )
    {
        final PropertyData[] array = datas( size );
        final int times = 10000000;
        measure( "scan", new Runnable()
        {
            @Override
            public void run()
            {
                for ( int i = 0; i < times; i++ )
                    doScan( array, i%array.length );
            }
        } );
        
        measure( "bs", new Runnable()
        {
            @Override
            public void run()
            {
                for ( int i = 0; i < times; i++ )
                    doBinarySearch( array, 0 );
            }
        } );
    }
    
    private PropertyData[] datas( int size )
    {
        PropertyData[] result = new PropertyData[size];
        for ( int i = 0; i < size; i++ )
            result[i] = PropertyDatas.forByte( i, 0, (byte) 0 );
        return result;
    }
    
    private PropertyData doScan( PropertyData[] array, int keyId )
    {
        for ( PropertyData pd : array )
        {
            if ( pd.getIndex() == keyId )
                return pd;
        }
        return null;
    }
    
    private PropertyData doBinarySearch( PropertyData[] array, int keyId )
    {
        // Copied from ArrayBasedPrimitive#getPropertyForIndex
        
        PropertyData[] local = array;
        
        // Algorithm copied from java.util.Arrays#binarySearch
        // Don't used used since the method signature makes it impossible
        // to use for PropertyData objects (where we compare to PropertyData#getIndex()
        int low = 0;
        int high = local.length-1;
        while ( low <= high )
        {
            int mid = (low + high) >>> 1;
            PropertyData midVal = local[mid];
            int midId = midVal.getIndex();

            if ( midId < keyId )
                low = mid + 1;
            else if ( midId > keyId )
                high = mid - 1;
            else
                return midVal; // key found
        }
        return null;
    }

    private void measure( String name, Runnable runnable )
    {
        // Warmup
        runnable.run();
        
        // Run
        System.out.print( name + "... " );
        Measurement m = new Measurement();
        for ( int i = 0; i < 3; i++ )
        {
            long t = System.currentTimeMillis();
            runnable.run();
            m.add( (System.currentTimeMillis()-t) );
        }
        System.out.println( name + ":" + m.average() + " (lower=better)" );
    }

    private class Measurement
    {
        private long time;
        private int count;
        
        public void add( long time )
        {
            this.time += time;
            this.count++;
        }
        
        public double average()
        {
            return (double)time/count;
        }
    }
}
