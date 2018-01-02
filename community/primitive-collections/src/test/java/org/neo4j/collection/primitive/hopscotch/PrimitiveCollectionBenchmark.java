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
package org.neo4j.collection.primitive.hopscotch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.test.randomized.RandomizedTester.TargetFactory;
import org.neo4j.test.randomized.TestResource;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;

@Ignore( "Not a test. A benchmark" )
public class PrimitiveCollectionBenchmark
{
    /* A tuesday afternoon run on MPs machine:

        TROVE
        testing: trove4j long->int map, random
          add: 1696, contains: 566, mem: 1154154496   0.37877269331638164
          add: 1881, contains: 614, mem: 1569521664   0.37877269331638164
          add: 1761, contains: 596, mem: 2332098560   0.37877269331638164
        testing: trove4j long->int map, seq
          add: 531, contains: 69, mem: 2517041152   0.37965116815437405
          add: 355, contains: 69, mem: 2519793664   0.37965116815437405
          add: 334, contains: 74, mem: 2517041152   0.37965116815437405
        testing: trove4j set, random
          add: 1231, contains: 714, mem: 2518220800   0.37877269331638164
          add: 1306, contains: 665, mem: 2517041152   0.37877269331638164
          add: 1343, contains: 678, mem: 2517499904   0.37877269331638164
        testing: trove4j set, seq
          add: 286, contains: 93, mem: 2517041152   0.37965116815437405
          add: 329, contains: 93, mem: 2517172224   0.37965116815437405
          add: 310, contains: 92, mem: 2517172224   0.37965116815437405

        COLT
        testing: colt long->int map, random
          add: 1765, contains: 693, mem: 1186922496   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@4990d0d2
          add: 1609, contains: 657, mem: 1086259200   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@599e80b1
          add: 1698, contains: 651, mem: 1634729984   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@ae03a4d
        testing: colt long->int map, seq
          add: 620, contains: 111, mem: 1955528704   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@62d790a8
          add: 624, contains: 109, mem: 2323382272   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@39be9f72
          add: 478, contains: 109, mem: 2325938176   org.neo4j.kernel.impl.util.hopscotch.PrimitiveCollectionBenchmark$ColtMap@1769261f

        NEO4J
        testing: neo4j hop-scotch long->int map, random
          add: 1669, contains: 812, mem: 813039616   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
          add: 1509, contains: 864, mem: 895942656   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
          add: 1470, contains: 791, mem: 1097334784   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
        testing: neo4j hop-scotch long->int map, seq
          add: 649, contains: 121, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]
          add: 649, contains: 119, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]
          add: 645, contains: 120, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]
        testing: neo4j hop-scotch set, random
          add: 1380, contains: 761, mem: 1097334784   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
          add: 1333, contains: 761, mem: 1097334784   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
          add: 1317, contains: 766, mem: 1097334784   int[]-table[capacity:16777216, size:9976861, usage:0.5946672558784485]
        testing: neo4j hop-scotch set, seq
          add: 594, contains: 152, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]
          add: 594, contains: 153, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]
          add: 593, contains: 151, mem: 1097334784   int[]-table[capacity:16777216, size:10000000, usage:0.5960464477539062]

     */

    private static final int RUNS = 3;

    @Test
    public void performanceTestPrimitiveLongSet() throws Exception
    {
        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
        {
            @Override
            public MapInterface newInstance()
            {
                return new HopScotchSet();
            }
        };
        performanceTest( "neo4j hop-scotch set, random", factory, RANDOM_DATA );
        performanceTest( "neo4j hop-scotch set, seq", factory, SEQUENTIAL_DATA );
    }

    @Test
    public void performanceTestPrimitiveLongMap() throws Exception
    {
        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
        {
            @Override
            public MapInterface newInstance()
            {
                return new HopScotchMap();
            }
        };
        performanceTest( "neo4j hop-scotch long->int map, random", factory, RANDOM_DATA );
        performanceTest( "neo4j hop-scotch long->int map, seq", factory, SEQUENTIAL_DATA );
    }

//    @Test
//    public void performanceTestColtLongMap() throws Exception
//    {
//        Factory<MapInterface> factory = new Factory<MapInterface>()
//        {
//            @Override
//            public MapInterface newInstance()
//            {
//                return new ColtMap();
//            }
//        };
//        performanceTest( "colt long->int map, random", factory, RANDOM_DATA );
//        performanceTest( "colt long->int map, seq", factory, SEQUENTIAL_DATA );
//    }

//    @Test
//    public void performanceTestTroveLongSet() throws Exception
//    {
//        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
//        {
//            @Override
//            public MapInterface newInstance()
//            {
//                return new TroveSet();
//            }
//        };
//        performanceTest( "trove4j set, random", factory, RANDOM_DATA );
//        performanceTest( "trove4j set, seq", factory, SEQUENTIAL_DATA );
//    }
//
//    @Test
//    public void performanceTestTroveLongMap() throws Exception
//    {
//        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
//        {
//            @Override
//            public MapInterface newInstance()
//            {
//                return new TroveMap();
//            }
//        };
//        performanceTest( "trove4j long->int map, random", factory, RANDOM_DATA );
//        performanceTest( "trove4j long->int map, seq", factory, SEQUENTIAL_DATA );
//    }

    @Test
    public void performanceTestJavaLongSet() throws Exception
    {
        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
        {
            @Override
            public MapInterface newInstance()
            {
                return new JucSet();
            }
        };
        performanceTest( "juc set, random", factory, RANDOM_DATA );
        performanceTest( "juc set, seq", factory, SEQUENTIAL_DATA );
    }

    @Test
    public void performanceTestJavaLongMap() throws Exception
    {
        TargetFactory<MapInterface> factory = new TargetFactory<MapInterface>()
        {
            @Override
            public MapInterface newInstance()
            {
                return new JucMap();
            }
        };
        performanceTest( "juc Long->Integer map, random", factory, RANDOM_DATA );
        performanceTest( "juc Long->Integer map, seq", factory, SEQUENTIAL_DATA );
    }

    private void performanceTest( String name, TargetFactory<MapInterface> factory,
            long[] data ) throws Exception
    {
        System.out.println( "testing: " + name );
        for ( int r = 0; r < RUNS; r++ )
        {
            // GIVEN
            try ( final MapInterface target = factory.newInstance() )
            {
                // WHEN
                long time = currentTimeMillis();
                long dataSize = data.length;
                for ( int i = 0; i < dataSize; i++ )
                {
                    target.put( data[i], (int)data[i] );
                }
                long addTime = currentTimeMillis() - time;
                time = currentTimeMillis();
                for ( int i = 0; i < dataSize; i++ )
                {
                    target.get( data[i] );
                }
                long containsTime = currentTimeMillis() - time;
                printResults( addTime, containsTime, target );
            }
        }
    }

    private interface MapInterface extends TestResource
    {
        void put( long key, int value );

        void get( long key );
    }

//    private static class ColtMap implements MapInterface
//    {
//        private final OpenLongIntHashMap map = new OpenLongIntHashMap();
//
//        @Override
//        public void put( long key, int value )
//        {
//            map.put( key, value );
//        }
//
//        @Override
//        public void get( long key )
//        {
//            map.get( key );
//        }
//    }

    private static class JucSet implements MapInterface
    {
        private final Set<Long> set = new HashSet<>();

        @Override
        public void put( long key, int value )
        {
            set.add( key );
        }

        @Override
        public void get( long key )
        {
            set.contains( key );
        }

        @Override
        public String toString()
        {
            return "" + set.size();
        }

        @Override
        public void close()
        {
        }
    }

    private static class JucMap implements MapInterface
    {
        private final Map<Long, Integer> map = new HashMap<>();

        @Override
        public void put( long key, int value )
        {
            map.put( key, value );
        }

        @Override
        public void get( long key )
        {
            map.get( key );
        }

        @Override
        public String toString()
        {
            return "" + map.size();
        }

        @Override
        public void close()
        {
        }
    }

//    private static class TroveSet implements MapInterface
//    {
//        private final TLongHashSet set = new TLongHashSet();
//
//        @Override
//        public void put( long key, int value )
//        {
//            set.add( key );
//        }
//
//        @Override
//        public void get( long key )
//        {
//            set.contains( key );
//        }
//
//        @Override
//        public String toString()
//        {
//            return "" + ((double)set.size() / (double)set.capacity());
//        }
//    }
//
//    private static class TroveMap implements MapInterface
//    {
//        private final TLongIntHashMap map = new TLongIntHashMap();
//
//        @Override
//        public void put( long key, int value )
//        {
//            map.put( key, value );
//        }
//
//        @Override
//        public void get( long key )
//        {
//            map.get( key );
//        }
//
//        @Override
//        public String toString()
//        {
//            return "" + ((double)map.size() / (double)map.capacity());
//        }
//    }

    private static class HopScotchSet implements MapInterface
    {
        private final PrimitiveLongSet set = Primitive.offHeapLongSet();

        @Override
        public void put( long key, int value )
        {
            set.add( key );
        }

        @Override
        public void get( long key )
        {
            set.contains( key );
        }

        @Override
        public String toString()
        {
            return set.toString();
        }

        @Override
        public void close()
        {
            set.close();
        }
    }

    private static class HopScotchMap implements MapInterface
    {
        private final PrimitiveLongIntMap map = Primitive.longIntMap();

        @Override
        public void put( long key, int value )
        {
            map.put( key, value );
        }

        @Override
        public void get( long key )
        {
            map.get( key );
        }

        @Override
        public String toString()
        {
            return map.toString();
        }

        @Override
        public void close()
        {
            map.close();
        }
    }

    private void printResults( long addTime, long containsTime, Object set ) throws Exception
    {
        for ( int i = 0; i < 5; i++ )
        {
            System.gc();
            sleep( 1000 );
        }
        System.out.println( "  add: " + addTime + ", contains: " + containsTime +
                ", mem: " + Runtime.getRuntime().totalMemory() + "   " + set );
    }

    private static final int DATA_SIZE = 10_000_000;
    private static final long[] RANDOM_DATA, SEQUENTIAL_DATA;
    static
    {
        RANDOM_DATA = new long[DATA_SIZE];
        SEQUENTIAL_DATA = new long[DATA_SIZE];
        Random random = new Random( 145878 /*picked at random, of course*/ );
        for ( int i = 0; i < DATA_SIZE; i++ )
        {
            RANDOM_DATA[i] = Math.abs( random.nextInt() );
            SEQUENTIAL_DATA[i] = i;
        }
    }
}
