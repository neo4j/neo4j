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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.util.function.ToIntFunction;

import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.values.storable.Value;

/**
 * Convenience wrapper for an {@link Input} that doesn't have support for multiple passes over its input streams,
 * {@link InputIterable#supportsMultiplePasses()}.
 */
public class CachedInput implements Input
{
    private final Input actual;
    private final InputIterable nodes;
    private final InputIterable relationships;

    private CachedInput( Input actual, InputCache cache )
    {
        this.actual = actual;
        this.nodes = new CachingInputIterable( actual.nodes(), cache );
        this.relationships = new CachingInputIterable( actual.relationships(), cache );
    }

    @Override
    public InputIterable nodes()
    {
        return nodes;
    }

    @Override
    public InputIterable relationships()
    {
        return relationships;
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return actual.idMapper( numberArrayFactory );
    }

    @Override
    public Collector badCollector()
    {
        return actual.badCollector();
    }

    public static Input cacheAsNecessary( Input input, InputCache cache )
    {
        return new CachedInput( input, cache );
    }

    @Override
    public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator ) throws IOException
    {
        return actual.calculateEstimates( valueSizeCalculator );
    }
}
