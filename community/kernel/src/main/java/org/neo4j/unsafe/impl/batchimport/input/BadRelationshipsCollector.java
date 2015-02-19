/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.OutputStream;
import java.io.PrintStream;

import static java.lang.String.format;

public class BadRelationshipsCollector implements Collector<InputRelationship>
{
    private final PrintStream out;
    private final int tolerance;

    // volatile since one importer thread calls collect(), where this value is incremented and later the "main"
    // thread calls badEntries() to get a count.
    private volatile int badEntries;

    public BadRelationshipsCollector( OutputStream out, int tolerance )
    {
        this.out = new PrintStream( out );
        this.tolerance = tolerance;
    }

    @Override
    public void collect( InputRelationship relationship, Object specificValue )
    {
        if ( ++badEntries > tolerance )
        {
            throw new InputException( format( "Too many bad entries, saw %d where last one was " +
                    "%s refering to missing node %s", badEntries, relationship, specificValue ) );
        }

        out.println( specificValue + " in " + relationship );
    }

    @Override
    public void close()
    {
        out.flush();
    }

    @Override
    public int badEntries()
    {
        return badEntries;
    }
}
