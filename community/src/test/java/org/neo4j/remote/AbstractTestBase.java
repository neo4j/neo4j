/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.remote;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.internal.builders.IgnoredClassRunner;
import org.junit.runner.RunWith;
import org.neo4j.graphdb.Transaction;

@RunWith( IgnoredClassRunner.class )
public abstract class AbstractTestBase
{
    static class GraphDatabaseConnectionFailedError extends Error
    {
        GraphDatabaseConnectionFailedError( Exception e )
        {
            super( "could not connect to graph database", e );
        }
    }

    private final Callable<RemoteGraphDatabase> factory;
    private RemoteGraphDatabase graphDb;

    protected AbstractTestBase( Callable<RemoteGraphDatabase> factory )
    {
        this.factory = factory;
    }

    protected void inTransaction( Runnable runnable )
    {
        Transaction tx = graphDb().beginTx();
        try
        {
            runnable.run();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    protected final synchronized RemoteGraphDatabase graphDb()
    {
        if ( graphDb == null )
        {
            try
            {
                graphDb = factory.call();
            }
            catch ( Exception e )
            {
                throw new GraphDatabaseConnectionFailedError( e );
            }
        }
        return graphDb;
    }

    protected RemoteIndexService indexService()
    {
        return new RemoteIndexService( graphDb(), "index" );
    }

    public static void assertArrayEquals( Object expected, Object actual )
    {
        if ( !arrayEquals( expected, actual ) )
        {
            fail( String.format( "expected <%s> got <%s>", expected, actual ) );
        }
    }

    public static void assertArrayEquals( String message, Object expected,
            Object actual )
    {
        if ( !arrayEquals( expected, actual ) )
        {
            fail( message );
        }
    }

    public static boolean arrayEquals( Object expected, Object actual )
    {
        return arrayEquals( expected, actual, false );
    }

    public static boolean arrayEquals( Object expected, Object actual, boolean unbox )
    {
        if ( expected instanceof Object[] )
        {
            if ( actual instanceof Object[] )
            {
                return Arrays.equals( (Object[]) expected, (Object[]) actual );
            }
            else if ( unbox )
            {
                return arrayEquals( actual, expected, true );
            }
        }
        else if ( expected instanceof boolean[] )
        {
            if ( actual instanceof boolean[] )
            {
                return Arrays.equals( (boolean[]) expected, (boolean[]) actual );
            }
            else if ( unbox && actual instanceof Boolean[] )
            {
                boolean[] exp = (boolean[]) expected;
                Boolean[] act = (Boolean[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof byte[] )
        {
            if ( actual instanceof byte[] )
            {
                return Arrays.equals( (byte[]) expected, (byte[]) actual );
            }
            else if ( unbox && actual instanceof Byte[] )
            {
                byte[] exp = (byte[]) expected;
                Byte[] act = (Byte[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof char[] )
        {
            if ( actual instanceof char[] )
            {
                return Arrays.equals( (char[]) expected, (char[]) actual );
            }
            else if ( unbox && actual instanceof Character[] )
            {
                char[] exp = (char[]) expected;
                Character[] act = (Character[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof short[] )
        {
            if ( actual instanceof short[] )
            {
                return Arrays.equals( (short[]) expected, (short[]) actual );
            }
            else if ( unbox && actual instanceof Short[] )
            {
                short[] exp = (short[]) expected;
                Short[] act = (Short[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof int[] )
        {
            if ( actual instanceof int[] )
            {
                return Arrays.equals( (int[]) expected, (int[]) actual );
            }
            else if ( unbox && actual instanceof Integer[] )
            {
                int[] exp = (int[]) expected;
                Integer[] act = (Integer[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof long[] )
        {
            if ( actual instanceof long[] )
            {
                return Arrays.equals( (long[]) expected, (long[]) actual );
            }
            else if ( unbox && actual instanceof Long[] )
            {
                long[] exp = (long[]) expected;
                Long[] act = (Long[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof float[] )
        {
            if ( actual instanceof float[] )
            {
                return Arrays.equals( (float[]) expected, (float[]) actual );
            }
            else if ( unbox && actual instanceof Float[] )
            {
                float[] exp = (float[]) expected;
                Float[] act = (Float[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        else if ( expected instanceof double[] )
        {
            if ( actual instanceof double[] )
            {
                return Arrays.equals( (double[]) expected, (double[]) actual );
            }
            else if ( unbox && actual instanceof Double[] )
            {
                double[] exp = (double[]) expected;
                Double[] act = (Double[]) actual;
                if ( exp.length == act.length )
                {
                    for ( int i = 0; i < exp.length; i++ )
                    {
                        if ( exp[i] != act[i] ) return false;
                    }
                    return true;
                }
            }
        }
        return false;
    }
}
