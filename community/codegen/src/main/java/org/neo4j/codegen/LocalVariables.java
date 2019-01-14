/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.codegen;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Repository of local variables.
 */
public class LocalVariables
{
    private final AtomicInteger counter = new AtomicInteger( 0 );
    private final Map<String,LocalVariable> localVariables = new HashMap<>();

    public LocalVariable createNew( TypeReference type, String name )
    {
        LocalVariable localVariable = new LocalVariable( type, name, counter.getAndIncrement() );
        localVariables.put( name, localVariable );
        //if 64 bit types we need to give it one more index
        if ( type.simpleName().equals( "double" ) || type.simpleName().equals( "long" ) )
        {
            counter.incrementAndGet();
        }
        return localVariable;
    }

    public LocalVariable get( String name )
    {
        LocalVariable localVariable = localVariables.get( name );
        if ( localVariable == null )
        {
            throw new NoSuchElementException( "No variable " + name + " in scope" );
        }
        return localVariable;
    }

    public static LocalVariables copy( LocalVariables original )
    {
        LocalVariables variables = new LocalVariables();
        variables.counter.set( original.counter.get() );
        original.localVariables.forEach( variables.localVariables::put );
        return variables;
    }
}
