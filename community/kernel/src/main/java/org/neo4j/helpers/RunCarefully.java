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
package org.neo4j.helpers;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.helpers.collection.Iterables.asArray;

public class RunCarefully
{
    private final Runnable[] operations;

    public RunCarefully( Runnable... operations )
    {
        this.operations = operations;
    }

    public RunCarefully( Iterable<Runnable> operations )
    {
        this( asArray( Runnable.class, operations ) );
    }

    public void run()
    {
        List<Throwable> errors = new ArrayList<>();

        for ( Runnable o : operations )
        {
            try
            {
                o.run();
            }
            catch ( RuntimeException e )
            {
                errors.add( e );
            }
        }

        if ( !errors.isEmpty() )
        {
            RuntimeException exception = new RuntimeException();
            errors.forEach( exception::addSuppressed );
            throw exception;
        }
    }
}
