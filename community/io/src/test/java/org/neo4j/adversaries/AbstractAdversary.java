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
package org.neo4j.adversaries;

import java.util.Random;

@SuppressWarnings( "unchecked" )
public abstract class AbstractAdversary implements Adversary
{
    protected final Random rng;

    public AbstractAdversary()
    {
        rng = new Random();
    }

    public void setSeed( long seed )
    {
        rng.setSeed( seed );
    }

    protected void throwOneOf( Class<? extends Throwable>... types )
    {
        int choice = rng.nextInt( types.length );
        Class<? extends Throwable> type = types[choice];
        Throwable throwable;
        try
        {
            throwable = type.newInstance();
        }
        catch ( Exception e )
        {
            throw new AssertionError( new Exception( "Failed to instantiate failure", e ) );
        }
        sneakyThrow( throwable );
    }

    public static void sneakyThrow(Throwable throwable)
    {
        AbstractAdversary.<RuntimeException>_sneakyThrow( throwable );
    }

    // http://youtu.be/7qXXWHfJha4
    private static <T extends Throwable> void _sneakyThrow( Throwable throwable ) throws T
    {
        throw (T) throwable;
    }
}
