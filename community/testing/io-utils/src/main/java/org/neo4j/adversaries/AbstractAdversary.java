/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.Optional;
import java.util.Random;

import static java.util.Optional.ofNullable;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;

@SuppressWarnings( "unchecked" )
public abstract class AbstractAdversary implements Adversary
{
    protected final Random rng;
    private volatile Throwable adversaryException;

    AbstractAdversary()
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
        try
        {
            adversaryException = type.getDeclaredConstructor().newInstance();
        }
        catch ( NoSuchMethodException e )
        {
            try
            {
                adversaryException = type.getDeclaredConstructor( String.class ).newInstance( "Injected failure" );
            }
            catch ( Exception e1 )
            {
                throw failToInjectError( e1 );
            }
        }
        catch ( Exception e )
        {
            throw failToInjectError( e );
        }
        rethrow( adversaryException );
    }

    private AssertionError failToInjectError( Exception e )
    {
        return new AssertionError( new Exception( "Failed to instantiate failure", e ) );
    }

    @Override
    public Optional<Throwable> getLastAdversaryException()
    {
        return ofNullable( adversaryException );
    }
}
