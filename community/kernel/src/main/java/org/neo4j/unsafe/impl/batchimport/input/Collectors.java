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

import java.io.OutputStream;
import java.util.function.Function;

import org.neo4j.io.NullOutputStream;

/**
 * Common implementations of {@link Collector}
 */
public class Collectors
{
    private Collectors()
    {
    }

    public static Collector silentBadCollector( long tolerance )
    {
        return silentBadCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Collector silentBadCollector( long tolerance, int collect )
    {
        return badCollector( NullOutputStream.NULL_OUTPUT_STREAM, tolerance, collect );
    }

    public static Collector badCollector( OutputStream out, long unlimitedTolerance )
    {
        return badCollector( out, unlimitedTolerance, BadCollector.COLLECT_ALL, false );
    }

    public static Collector badCollector( OutputStream out, long tolerance, int collect )
    {
        return new BadCollector( out, tolerance, collect, false );
    }

    public static Collector badCollector( OutputStream out, long unlimitedTolerance, int collect, boolean skipBadEntriesLogging )
    {
        return new BadCollector( out, unlimitedTolerance, collect, skipBadEntriesLogging );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance )
    {
        return badCollector( tolerance, BadCollector.COLLECT_ALL );
    }

    public static Function<OutputStream,Collector> badCollector( final int tolerance, final int collect )
    {
        return out -> badCollector( out, tolerance, collect, false );
    }

    public static int collect( boolean skipBadRelationships, boolean skipDuplicateNodes, boolean ignoreExtraColumns )
    {
        return (skipBadRelationships ? BadCollector.BAD_RELATIONSHIPS : 0 ) |
               (skipDuplicateNodes ? BadCollector.DUPLICATE_NODES : 0 ) |
               (ignoreExtraColumns ? BadCollector.EXTRA_COLUMNS : 0 );
    }
}
