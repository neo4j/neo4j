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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Test;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PrimitiveLongSetRIT
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
            RandomizedTester<Sets,String> actions =
                    new RandomizedTester<>( setFactory(), actionFactory( random ) );

            Result<Sets,String> result = actions.run( max );
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

    private void fullVerification( Sets target, Random random )
    {
        for ( Long value : target.normalSet )
        {
            assertTrue( target.set.contains( value.longValue() ) );
        }

        for ( int i = 0; i < target.normalSet.size(); i++ )
        {
            assertFalse( target.set.contains( randomNonExisting( random, target.normalSet ) ) );
        }
    }

    private Printable given()
    {
        return new Printable()
        {
            @Override
            public void print( LinePrinter out )
            {
                out.println( PrimitiveLongSet.class.getSimpleName() + " set = " +
                        Primitive.class.getSimpleName() + ".longSet();" );
            }
        };
    }

    private ActionFactory<Sets,String> actionFactory( final Random random )
    {
        return new ActionFactory<Sets,String>()
        {
            @Override
            public Action<Sets,String> apply( Sets from )
            {
                return generateAction( random, from );
            }
        };
    }

    private TargetFactory<Sets> setFactory()
    {
        return new TargetFactory<Sets>()
        {
            @Override
            public Sets newInstance()
            {
                return new Sets();
            }
        };
    }

    protected Action<Sets,String> generateAction( Random random, Sets from )
    {
        boolean anExisting = !from.normalSet.isEmpty() && random.nextInt( 3 ) == 0;
        long value = anExisting ?
                randomExisting( random, from.normalSet ) :
                randomNonExisting( random, from.normalSet );

        int typeOfAction = random.nextInt( 5 );
        if ( typeOfAction == 0 )
        {   // remove
            return new RemoveAction( value );
        }

        // add
        return new AddAction( value );
    }

    private long randomNonExisting( Random random, Set<Long> existing )
    {
        while ( true )
        {
            long value = Math.abs( random.nextLong() );
            if ( !existing.contains( value ) )
            {
                return value;
            }
        }
    }

    private long randomExisting( Random random, Set<Long> existing )
    {
        int index = random.nextInt( existing.size() )+1;
        Iterator<Long> iterator = existing.iterator();
        long value = 0;
        for ( int i = 0; i < index; i++ )
        {
            value = iterator.next();
        }
        return value;
    }

    private static class AddAction implements Action<Sets,String>
    {
        private final long value;

        AddAction( long value )
        {
            this.value = value;
        }

        @Override
        public String apply( Sets target )
        {
            try
            {
                boolean alreadyExisting = target.normalSet.contains( value );

                PrimitiveLongSet set = target.set;
                boolean existedBefore = set.contains( value );
                boolean added = set.add( value );
                boolean existsAfter = set.contains( value );
                target.normalSet.add( value );

                boolean ok = (existedBefore == alreadyExisting) & (added == !alreadyExisting) & existsAfter;
                return ok ? null : "" + value + alreadyExisting + "," + existedBefore + "," + added + "," + existsAfter;
            }
            catch ( Exception e )
            {
                return "exception:" + e.getMessage();
            }
        }

        @Override
        public void printAsCode( Sets source, LinePrinter out, boolean includeChecks )
        {
            boolean alreadyExisting = source.normalSet.contains( value );
            String addition = "set.add( " + value + "L );";
            if ( includeChecks )
            {
                out.println( format( "boolean existedBefore = set.contains( %dL );", value ) );
                out.println( format( "boolean added = %s", addition ) );
                out.println( format( "boolean existsAfter = set.contains( %dL );", value ) );
                out.println( format( "assert%s( \"%s\", existedBefore );", capitilize( alreadyExisting ),
                        value + " should " + (alreadyExisting?"":"not ") + "exist before adding here" ) );
                out.println( format( "assert%s( \"%s\", added );", capitilize( !alreadyExisting ),
                        value + " should " + (!alreadyExisting?"":"not ") + "be reported as added here" ) );
                out.println( format( "assertTrue( \"%s\", existsAfter );",
                        value + " should exist" ) );
            }
            else
            {
                out.println( addition );
            }
        }
    }

    private static class RemoveAction implements Action<Sets,String>
    {
        private final long value;

        RemoveAction( long value )
        {
            this.value = value;
        }

        @Override
        public String apply( Sets target )
        {
            try
            {
                boolean alreadyExisting = target.normalSet.contains( value );
                PrimitiveLongSet set = target.set;
                boolean existedBefore = set.contains( value );
                boolean removed = set.remove( value );
                boolean existsAfter = set.contains( value );
                target.normalSet.remove( value );

                boolean ok = (existedBefore == alreadyExisting) & (removed == alreadyExisting) & !existsAfter;
                return ok ? null : "" + value + alreadyExisting + "," + existedBefore + "," + removed + "," + existsAfter;
            }
            catch ( Exception e )
            {
                return "exception: " + e.getMessage();
            }
        }

        @Override
        public void printAsCode( Sets source, LinePrinter out, boolean includeChecks )
        {
            boolean alreadyExisting = source.normalSet.contains( value );
            String removal = "set.remove( " + value + "L );";
            if ( includeChecks )
            {
                out.println( format( "boolean existedBefore = set.contains( %dL );", value ) );
                out.println( format( "boolean removed = %s", removal ) );
                out.println( format( "boolean existsAfter = set.contains( %dL );", value ) );
                out.println( format( "assert%s( \"%s\", existedBefore );", capitilize( alreadyExisting ),
                        value + " should " + (alreadyExisting?"":"not ") + "exist before removing here" ) );
                out.println( format( "assert%s( \"%s\", removed );", capitilize( alreadyExisting ),
                        value + " should " + (alreadyExisting?"":"not ") + "be reported as removed here" ) );
                out.println( format( "assertFalse( \"%s\", existsAfter );",
                        value + " should not exist" ) );
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

    private static class Sets implements TestResource
    {
        final Set<Long> normalSet = new HashSet<>();
        final PrimitiveLongSet set = Primitive.longSet();

        @Override
        public void close()
        {
            set.close();
        }
    }
}
