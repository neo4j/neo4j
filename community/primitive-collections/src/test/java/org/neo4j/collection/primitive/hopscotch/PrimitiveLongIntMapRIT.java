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
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.test.randomized.Action;
import org.neo4j.test.randomized.LinePrinter;
import org.neo4j.test.randomized.Printable;
import org.neo4j.test.randomized.RandomizedTester;
import org.neo4j.test.randomized.RandomizedTester.ActionFactory;
import org.neo4j.test.randomized.RandomizedTester.TargetFactory;
import org.neo4j.test.randomized.Result;
import org.neo4j.test.randomized.TestResource;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrimitiveLongIntMapRIT
{
    @Test
    public void thoroughlyTestIt() throws Exception
    {
        long endTime = currentTimeMillis() + SECONDS.toMillis( 5 );
        while ( currentTimeMillis() < endTime )
        {
            long seed = currentTimeMillis();
            final Random random = new Random( seed );
            int max = random.nextInt( 10_000 ) + 100;
            RandomizedTester<Maps,String> actions =
                    new RandomizedTester<>( mapFactory(), actionFactory( random ) );

            Result<Maps,String> result = actions.run( max );
            if ( result.isFailure() )
            {
                System.out.println( "Found failure at " + result );
                actions.testCaseWriter( "shouldOnlyContainAddedValues", given() ).print( System.out );
                System.out.println( "Actually, minimal reproducible test of that is..." );
                actions.findMinimalReproducible().testCaseWriter( "shouldOnlyContainAddedValues",
                        given() ).print( System.out );
                fail( "Failed, see printed test case for how to reproduce. Seed:" + seed );
            }

            fullVerification( result.getTarget(), random );
        }
    }

    private void fullVerification( Maps target, Random random )
    {
        for ( Map.Entry<Long,Integer> entry : target.normalMap.entrySet() )
        {
            assertTrue( target.map.containsKey( entry.getKey() ) );
            assertEquals( entry.getValue().intValue(), target.map.get( entry.getKey() ) );
        }

        for ( int i = 0; i < target.normalMap.size(); i++ )
        {
            assertFalse( target.map.containsKey( randomNonExisting( random, target.normalMap ) ) );
        }
    }

    private Printable given()
    {
        return new Printable()
        {
            @Override
            public void print( LinePrinter out )
            {
                out.println( PrimitiveLongIntMap.class.getSimpleName() + " map = " +
                        Primitive.class.getSimpleName() + ".longIntMap();" );
            }
        };
    }

    private ActionFactory<Maps,String> actionFactory( final Random random )
    {
        return new ActionFactory<Maps,String>()
        {
            @Override
            public Action<Maps,String> apply( Maps from )
            {
                return generateAction( random, from );
            }
        };
    }

    private TargetFactory<Maps> mapFactory()
    {
        return new TargetFactory<Maps>()
        {
            @Override
            public Maps newInstance()
            {
                return new Maps();
            }
        };
    }

    protected Action<Maps,String> generateAction( Random random, Maps from )
    {
        boolean anExisting = !from.normalMap.isEmpty() && random.nextInt( 3 ) == 0;
        long key = anExisting ?
                randomExisting( random, from.normalMap ) :
                randomNonExisting( random, from.normalMap );
        Integer value = random.nextInt( 100 );

        int typeOfAction = random.nextInt( 5 );
        if ( typeOfAction == 0 )
        {   // remove
            return new RemoveAction( key );
        }

        // add
        return new AddAction( key, value );
    }

    private long randomNonExisting( Random random, Map<Long,Integer> existing )
    {
        while ( true )
        {
            long key = Math.abs( random.nextLong() );
            if ( !existing.containsKey( key ) )
            {
                return key;
            }
        }
    }

    private long randomExisting( Random random, Map<Long,Integer> existing )
    {
        int index = random.nextInt( existing.size() )+1;
        Iterator<Long> iterator = existing.keySet().iterator();
        long value = 0;
        for ( int i = 0; i < index; i++ )
        {
            value = iterator.next();
        }
        return value;
    }

    private static class AddAction implements Action<Maps,String>
    {
        private final long key;
        private final int value;

        AddAction( long key, int value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public String apply( Maps target )
        {
            boolean existing = target.normalMap.containsKey( key );
            int existingValue = existing ? target.normalMap.get( key ) : -1;
            int actualSizeBefore = target.normalMap.size();

            int sizeBefore = target.map.size();
            boolean existedBefore = target.map.containsKey( key );
            int valueBefore = target.map.get( key );
            int previous = target.map.put( key, value );
            boolean existsAfter = target.map.containsKey( key );
            int valueAfter = target.map.get( key );
            target.normalMap.put( key, value );
            int sizeAfter = target.map.size();

            int actualSizeAfter = target.normalMap.size();
            boolean ok =
                    (sizeBefore == actualSizeBefore) &
                    (existedBefore == existing) &
                    (existingValue == valueBefore) &
                    (existingValue == previous) &
                    (valueAfter == value) &
                    existsAfter &
                    (sizeAfter == actualSizeAfter);
            return ok ? null : "" + key + ":" + value + "," + existingValue + "," + existedBefore +
                    "," + previous + "," + existsAfter;
        }

        @Override
        public void printAsCode( Maps source, LinePrinter out, boolean includeChecks )
        {
            String addition = "map.put( " + key + ", " + value + " );";
            if ( includeChecks )
            {
                boolean existing = source.normalMap.containsKey( key );
                int existingValue = existing ? source.normalMap.get( key ) : -1;
                out.println( format( "int sizeBefore = map.size();" ) );
                out.println( format( "boolean existedBefore = map.containsKey( %d );", key ) );
                out.println( format( "int valueBefore = map.get( %d );", key ) );
                out.println( format( "int previous = %s", addition ) );
                out.println( format( "boolean existsAfter = map.containsKey( %d );", key ) );
                out.println( format( "int valueAfter = map.get( %d );", key ) );
                out.println( format( "int sizeAfter = map.size();" ) );

                int actualSizeBefore = source.normalMap.size();
                out.println( format( "assertEquals( \"%s\", %d, sizeBefore );",
                        "Size before put should have been " + actualSizeBefore, actualSizeBefore ) );
                out.println( format( "assert%s( \"%s\", existedBefore );", capitilize( existing ),
                        key + " should " + (existing?"":"not ") + "exist before putting here" ) );
                out.println( format( "assertEquals( \"%s\", %d, valueBefore );",
                        "value before should be " + existingValue, existingValue ) );
                out.println( format( "assertEquals( \"%s\", %d, previous );",
                        "value returned from put should be " + existingValue, existingValue ) );
                out.println( format( "assertTrue( \"%s\", existsAfter );",
                        key + " should exist" ) );
                out.println( format( "assertEquals( \"%s\", %d, valueAfter );",
                        "value after putting should be " + value, value ) );
                int actualSizeAfter = existing ? actualSizeBefore : actualSizeBefore+1;
                out.println( format( "assertEquals( \"%s\", %d, sizeAfter );",
                        "Size after put should have been " + actualSizeAfter, actualSizeAfter ) );
            }
            else
            {
                out.println( addition );
            }
        }
    }

    private static class RemoveAction implements Action<Maps,String>
    {
        private final long key;

        RemoveAction( long key )
        {
            this.key = key;
        }

        @Override
        public String apply( Maps target )
        {
            boolean existing = target.normalMap.containsKey( key );
            int existingValue = existing ? target.normalMap.get( key ) : -1;

            boolean existedBefore = target.map.containsKey( key );
            int valueBefore = target.map.get( key );
            int removed = target.map.remove( key );
            boolean existsAfter = target.map.containsKey( key );
            int valueAfter = target.map.get( key );
            target.normalMap.remove( key );

            boolean ok =
                    (existedBefore == existing) &
                    (existingValue == valueBefore) &
                    (existingValue == removed) &
                    (valueAfter == -1) &
                    !existsAfter;
            return ok ? null : "" + key + "," + existingValue + "," + existedBefore +
                    "," + removed + "," + existsAfter;
        }

        @Override
        public void printAsCode( Maps source, LinePrinter out, boolean includeChecks )
        {
            String removal = "map.remove( " + key + " );";
            if ( includeChecks )
            {
                boolean existing = source.normalMap.containsKey( key );
                int existingValue = existing ? source.normalMap.get( key ) : -1;
                out.println( format( "boolean existedBefore = map.containsKey( %d );", key ) );
                out.println( format( "int valueBefore = map.get( %d );", key ) );
                out.println( format( "int removed = %s", removal ) );
                out.println( format( "boolean existsAfter = map.containsKey( %d );", key ) );
                out.println( format( "int valueAfter = map.get( %d );", key ) );

                out.println( format( "assert%s( \"%s\", existedBefore );", capitilize( existing ),
                        key + " should " + (existing?"":"not ") + "exist before removing here" ) );
                out.println( format( "assertEquals( \"%s\", %d, valueBefore );",
                        "value before should be " + existingValue, existingValue ) );
                out.println( format( "assertEquals( \"%s\", %d, removed );",
                        "value returned from remove should be " + existingValue, existingValue ) );
                out.println( format( "assertFalse( \"%s\", existsAfter );",
                        key + " should not exist" ) );
                out.println( format( "assertEquals( \"%s\", -1, valueAfter );",
                        "value after removing should be -1" ) );
            }
            else
            {
                out.println( removal );
            }
        }
    }

    private static String capitilize( boolean bool )
    {
        String string = Boolean.valueOf( bool ).toString();
        return string.substring( 0, 1 ).toUpperCase() + string.substring( 1 ).toLowerCase();
    }

    private static class Maps implements TestResource
    {
        final Map<Long,Integer> normalMap = new HashMap<>();
        final PrimitiveLongIntMap map = Primitive.longIntMap();

        @Override
        public String toString()
        {
            return map.toString();
        }

        @Override
        public void close()
        {
        }
    }
}
