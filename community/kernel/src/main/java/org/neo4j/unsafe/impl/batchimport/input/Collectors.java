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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.function.Function;

/**
 * Common implementations of {@link Collector}
 */
public class Collectors
{
    public static Collector silentBadCollector( int tolerance )
    {
        return silentBadCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Collector silentBadCollector( int tolerance, int collect )
    {
        return badCollector( new OutputStream()
        {
            @Override
            public void write( int i ) throws IOException
            {
                // ignored
            }
        }, tolerance, collect );
    }

    public static Collector badCollector( OutputStream out, int tolerance )
    {
        return badCollector( out, tolerance, BadCollector.COLLECT_ALL );
    }

    public static Collector badCollector( OutputStream out, int tolerance, int collect )
    {
        return new BadCollector( out, tolerance, collect );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance )
    {
        return badCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance, final int collect )
    {
        return new Function<OutputStream,Collector>()
        {
            @Override
            public Collector apply( OutputStream out ) throws RuntimeException
            {
                return badCollector( out, tolerance, collect );
            }
        };
    }

    public static int collect( boolean skipBadRelationships, boolean skipDuplicateNodes, boolean ignoreExtraColumns )
    {
        return (skipBadRelationships ? BadCollector.BAD_RELATIONSHIPS : 0 ) |
               (skipDuplicateNodes ? BadCollector.DUPLICATE_NODES : 0 ) |
               (ignoreExtraColumns ? BadCollector.EXTRA_COLUMNS : 0 );
    }
}
