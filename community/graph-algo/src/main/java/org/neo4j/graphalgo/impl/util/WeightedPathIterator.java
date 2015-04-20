/*
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
package org.neo4j.graphalgo.impl.util;

import java.util.Iterator;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Path;
import org.neo4j.helpers.collection.PrefetchingIterator;

public class WeightedPathIterator extends PrefetchingIterator<WeightedPath>
{
    private final Iterator<Path> paths;
    private final CostEvaluator<Double> costEvaluator;
    private Double foundWeight;
    private final boolean stopAfterLowestWeight;

    public WeightedPathIterator( Iterator<Path> paths, CostEvaluator<Double> costEvaluator,
            boolean stopAfterLowestWeight )
    {
        this.paths = paths;
        this.costEvaluator = costEvaluator;
        this.stopAfterLowestWeight = stopAfterLowestWeight;
    }

    @Override
    protected WeightedPath fetchNextOrNull()
    {
        if ( !paths.hasNext() )
        {
            return null;
        }
        WeightedPath path = new WeightedPathImpl( costEvaluator, paths.next() );
        if ( stopAfterLowestWeight && foundWeight != null && path.weight() > foundWeight )
        {
            return null;
        }
        foundWeight = path.weight();
        return path;
    }
}
