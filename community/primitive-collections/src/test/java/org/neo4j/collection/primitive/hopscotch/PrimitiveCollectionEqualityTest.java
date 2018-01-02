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

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveCollection;
import org.neo4j.collection.primitive.PrimitiveIntLongMap;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.collection.primitive.PrimitiveLongLongMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.function.Factory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

@SuppressWarnings( "unchecked" )
@RunWith( Theories.class )
public class PrimitiveCollectionEqualityTest
{
    private static interface Value<T extends PrimitiveCollection>
    {
        void add( T coll );

        /**
         * @return 'true' if what was removed was exactly the value that was put in.
         */
        boolean remove( T coll );
    }

    private static abstract class ValueProducer<T extends PrimitiveCollection>
    {
        private final Class<T> applicableType;

        public ValueProducer( Class<T> applicableType )
        {
            this.applicableType = applicableType;
        }

        public boolean isApplicable( Factory<? extends PrimitiveCollection> factory )
        {
            try ( PrimitiveCollection coll = factory.newInstance() )
            {
                return applicableType.isInstance( coll );
            }
        }

        public abstract Value<T> randomValue();
    }


    // ==== Test Value Producers ====

    @DataPoint
    public static ValueProducer<PrimitiveIntSet> intV = new ValueProducer<PrimitiveIntSet>( PrimitiveIntSet.class )
    {
        @Override
        public Value<PrimitiveIntSet> randomValue()
        {
            final int x = randomInt();
            return new Value<PrimitiveIntSet>()
            {
                @Override
                public void add( PrimitiveIntSet coll )
                {
                    coll.add( x );
                }

                @Override
                public boolean remove( PrimitiveIntSet coll )
                {
                    return coll.remove( x );
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveLongSet> longV =
            new ValueProducer<PrimitiveLongSet>( PrimitiveLongSet.class )
    {
        @Override
        public Value<PrimitiveLongSet> randomValue()
        {
            final long x = randomLong();
            return new Value<PrimitiveLongSet>()
            {
                @Override
                public void add( PrimitiveLongSet coll )
                {
                    coll.add( x );
                }

                @Override
                public boolean remove( PrimitiveLongSet coll )
                {
                    return coll.remove( x );
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveIntLongMap> intLongV = new ValueProducer<PrimitiveIntLongMap>(
            PrimitiveIntLongMap.class )
    {
        @Override
        public Value<PrimitiveIntLongMap> randomValue()
        {
            final int x = randomInt();
            final long y = randomLong();
            return new Value<PrimitiveIntLongMap>()
            {
                @Override
                public void add( PrimitiveIntLongMap coll )
                {
                    coll.put( x, y );
                }

                @Override
                public boolean remove( PrimitiveIntLongMap coll )
                {
                    return coll.remove( x ) == y;
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveLongIntMap> longIntV = new ValueProducer<PrimitiveLongIntMap>(
            PrimitiveLongIntMap.class )
    {
        @Override
        public Value<PrimitiveLongIntMap> randomValue()
        {
            final long x = randomLong();
            final int y = randomInt();
            return new Value<PrimitiveLongIntMap>()
            {
                @Override
                public void add( PrimitiveLongIntMap coll )
                {
                    coll.put( x, y );
                }

                @Override
                public boolean remove( PrimitiveLongIntMap coll )
                {
                    return coll.remove( x ) == y;
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveLongLongMap> longLongV = new ValueProducer<PrimitiveLongLongMap>(
            PrimitiveLongLongMap.class )
    {
        @Override
        public Value<PrimitiveLongLongMap> randomValue()
        {
            final long x = randomLong();
            final long y = randomLong();
            return new Value<PrimitiveLongLongMap>()
            {
                @Override
                public void add( PrimitiveLongLongMap coll )
                {
                    coll.put( x, y );
                }

                @Override
                public boolean remove( PrimitiveLongLongMap coll )
                {
                    return coll.remove( x ) == y;
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveIntObjectMap> intObjV =
            new ValueProducer<PrimitiveIntObjectMap>( PrimitiveIntObjectMap.class )
    {
        @Override
        public Value<PrimitiveIntObjectMap> randomValue()
        {
            final int x = randomInt();
            final Object y = new Object();
            return new Value<PrimitiveIntObjectMap>()
            {
                @Override
                public void add( PrimitiveIntObjectMap coll )
                {
                    coll.put( x, y );
                }

                @Override
                public boolean remove( PrimitiveIntObjectMap coll )
                {
                    return coll.remove( x ) == y;
                }
            };
        }
    };

    @DataPoint
    public static ValueProducer<PrimitiveLongObjectMap> longObjV =
            new ValueProducer<PrimitiveLongObjectMap>( PrimitiveLongObjectMap.class )
    {
        @Override
        public Value<PrimitiveLongObjectMap> randomValue()
        {
            final long x = randomLong();
            final Object y = new Object();
            return new Value<PrimitiveLongObjectMap>()
            {
                @Override
                public void add( PrimitiveLongObjectMap coll )
                {
                    coll.put( x, y );
                }

                @Override
                public boolean remove( PrimitiveLongObjectMap coll )
                {
                    return coll.remove( x ) == y;
                }
            };
        }
    };


    // ==== Primitive Collection Implementations ====

    @DataPoint
    public static Factory<PrimitiveIntSet> intSet = new Factory<PrimitiveIntSet>()
    {
        @Override
        public PrimitiveIntSet newInstance()
        {
            return Primitive.intSet();
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntSet> intSetWithCapacity = new Factory<PrimitiveIntSet>()
    {
        @Override
        public PrimitiveIntSet newInstance()
        {
            return Primitive.intSet( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntSet> offheapIntSet = new Factory<PrimitiveIntSet>()
    {
        @Override
        public PrimitiveIntSet newInstance()
        {
            return Primitive.offHeapIntSet();
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntSet> offheapIntSetWithCapacity = new Factory<PrimitiveIntSet>()
    {
        @Override
        public PrimitiveIntSet newInstance()
        {
            return Primitive.offHeapIntSet( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongSet> longSet = new Factory<PrimitiveLongSet>()
    {
        @Override
        public PrimitiveLongSet newInstance()
        {
            return Primitive.longSet();
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongSet> longSetWithCapacity = new Factory<PrimitiveLongSet>()
    {
        @Override
        public PrimitiveLongSet newInstance()
        {
            return Primitive.longSet( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongSet> offheapLongSet = new Factory<PrimitiveLongSet>()
    {
        @Override
        public PrimitiveLongSet newInstance()
        {
            return Primitive.offHeapLongSet();
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongSet> offheapLongSetWithCapacity = new Factory<PrimitiveLongSet>()
    {
        @Override
        public PrimitiveLongSet newInstance()
        {
            return Primitive.offHeapLongSet( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntLongMap> intLongMap = new Factory<PrimitiveIntLongMap>()
    {
        @Override
        public PrimitiveIntLongMap newInstance()
        {
            return Primitive.intLongMap();
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntLongMap> intLongMapWithCapacity = new Factory<PrimitiveIntLongMap>()
    {
        @Override
        public PrimitiveIntLongMap newInstance()
        {
            return Primitive.intLongMap( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongIntMap> longIntMap = new Factory<PrimitiveLongIntMap>()
    {
        @Override
        public PrimitiveLongIntMap newInstance()
        {
            return Primitive.longIntMap();
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongIntMap> longIntMapWithCapacity = new Factory<PrimitiveLongIntMap>()
    {
        @Override
        public PrimitiveLongIntMap newInstance()
        {
            return Primitive.longIntMap( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongLongMap> offheapLongLongMap = new Factory<PrimitiveLongLongMap>()
    {
        @Override
        public PrimitiveLongLongMap newInstance()
        {
            return Primitive.offHeapLongLongMap();
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongLongMap> offheapLongLongMapWithCapacity = new Factory<PrimitiveLongLongMap>()
    {
        @Override
        public PrimitiveLongLongMap newInstance()
        {
            return Primitive.offHeapLongLongMap( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntObjectMap> intObjMap = new Factory<PrimitiveIntObjectMap>()
    {
        @Override
        public PrimitiveIntObjectMap newInstance()
        {
            return Primitive.intObjectMap();
        }
    };

    @DataPoint
    public static Factory<PrimitiveIntObjectMap> intObjMapWithCapacity = new Factory<PrimitiveIntObjectMap>()
    {
        @Override
        public PrimitiveIntObjectMap newInstance()
        {
            return Primitive.intObjectMap( randomCapacity() );
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongObjectMap> longObjectMap = new Factory<PrimitiveLongObjectMap>()
    {
        @Override
        public PrimitiveLongObjectMap newInstance()
        {
            return Primitive.longObjectMap();
        }
    };

    @DataPoint
    public static Factory<PrimitiveLongObjectMap> longObjectMapWithCapacity = new Factory<PrimitiveLongObjectMap>()
    {
        @Override
        public PrimitiveLongObjectMap newInstance()
        {
            return Primitive.longObjectMap( randomCapacity() );
        }
    };

    private static final PrimitiveIntSet observedRandomInts = Primitive.intSet();
    private static final PrimitiveLongSet observedRandomLongs = Primitive.longSet();

    /**
     * Produce a random int that hasn't been seen before by any test.
     */
    private static int randomInt()
    {
        int n;
        do
        {
            n = ThreadLocalRandom.current().nextInt();
        }
        while ( n == -1 || !observedRandomInts.add( n ) );
        return n;
    }

    /**
     * Produce a random long that hasn't been seen before by any test.
     */
    private static long randomLong()
    {
        long n;
        do
        {
            n = ThreadLocalRandom.current().nextLong();
        }
        while ( n == -1 || !observedRandomLongs.add( n ) );
        return n;
    }

    private static int randomCapacity()
    {
        return ThreadLocalRandom.current().nextInt( 30, 1200 );
    }

    private void assertEquals( PrimitiveCollection a, PrimitiveCollection b )
    {
        assertThat( a, is( equalTo( b ) ) );
        assertThat( b, is( equalTo( a ) ) );
        assertThat( a.hashCode(), is( equalTo( b.hashCode() ) ) );
    }

    @Theory
    public void collectionsAreNotEqualToObjectsOfOtherTypes( Factory<PrimitiveCollection> factory )
    {
        Object coll = factory.newInstance();
        assertNotEquals( coll, new Object() );
    }

    @Theory
    public void emptyCollectionsAreEqual(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
              PrimitiveCollection b = factoryB.newInstance() )
        {
            assertEquals( a, b );
        }
    }

    @Theory
    public void addingTheSameValuesMustProduceEqualCollections(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
              PrimitiveCollection b = factoryB.newInstance() )
        {
            Value value = values.randomValue();
            value.add( a );
            value.add( b );
            assertEquals( a, b );
        }
    }

    @Theory
    public void addingDifferentValuesMustProduceUnequalCollections(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
              PrimitiveCollection b = factoryB.newInstance() )
        {
            values.randomValue().add( a );
            values.randomValue().add( b );
            assertNotEquals( a, b );
        }
    }

    @Theory
    public void differentButEquivalentMutationsShouldProduceEqualCollections(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        // Note that this test, cute as it is, also verifies that the hashCode implementation is order-invariant :)
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
              PrimitiveCollection b = factoryB.newInstance() )
        {
            Value x = values.randomValue();
            Value y = values.randomValue();
            Value z = values.randomValue();

            x.add( a );
            z.add( a );

            z.add( b );
            y.add( b );
            x.add( b );
            y.remove( b );

            assertEquals( a, b );
        }
    }

    @Theory
    public void capacityDifferencesMustNotInfluenceEquality(
            ValueProducer values, Factory<PrimitiveCollection> factoryA, Factory<PrimitiveCollection> factoryB )
    {
        assumeTrue( values.isApplicable( factoryA ) );
        assumeTrue( values.isApplicable( factoryB ) );
        try ( PrimitiveCollection a = factoryA.newInstance();
              PrimitiveCollection b = factoryB.newInstance() )
        {
            List<Value> tmps = new ArrayList<>();
            for ( int i = 0; i < 5000; i++ )
            {
                Value value = values.randomValue();
                value.add( b );
                tmps.add( value );
            }

            Value specificValue = values.randomValue();
            specificValue.add( a );
            specificValue.add( b );

            for ( int i = 0; i < 5000; i++ )
            {
                Value value = values.randomValue();
                value.add( b );
                tmps.add( value );
            }

            Collections.shuffle( tmps );
            for ( Value value : tmps )
            {
                value.remove( b );
            }

            assertEquals( a, b );
        }
    }

    @Theory
    public void hashCodeMustFollowValues(
            ValueProducer values, Factory<PrimitiveCollection> factory )
    {
        assumeTrue( values.isApplicable( factory ) );
        try ( PrimitiveCollection a = factory.newInstance() )
        {
            Value x = values.randomValue();
            Value y = values.randomValue();
            Value z = values.randomValue();

            int i = a.hashCode();
            x.add( a );
            int j = a.hashCode();
            y.add( a );
            int k = a.hashCode();
            z.add( a );
            int l = a.hashCode();
            z.remove( a );
            int m = a.hashCode();
            y.remove( a );
            int n = a.hashCode();
            x.remove( a );
            int o = a.hashCode();

            assertThat( "0 elm hashcode equal", o, is( i ) );
            assertThat( "1 elm hashcode equal", n, is( j ) );
            assertThat( "2 elm hashcode equal", m, is( k ) );
            assertThat( "3 elm hashcode distinct", l, not( isOneOf( i, j, k, m, n, o ) ) );
            assertThat( "2 elm hashcode distinct", k, not( isOneOf( i, j, l, n, o ) ) );
            assertThat( "1 elm hashcode distinct", n, not( isOneOf( i, k, l, m, o ) ) );
            assertThat( "0 elm hashcode distinct", i, not( isOneOf( j, k, l, m, n ) ) );
        }
    }
}
