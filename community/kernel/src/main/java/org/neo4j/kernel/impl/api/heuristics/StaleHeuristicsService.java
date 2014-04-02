/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.heuristics;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.util.statistics.LabelledDistribution;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Exposes regular heuristics, but does not actively gather new heuristics.
 */
public class StaleHeuristicsService extends LifecycleAdapter implements HeuristicsService
{
    private final HeuristicsService delegate;

    public StaleHeuristicsService( HeuristicsService delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public LabelledDistribution<Integer> labelDistribution()
    {
        return delegate.labelDistribution();
    }

    @Override
    public LabelledDistribution<Integer> relationshipTypeDistribution()
    {
        return delegate.relationshipTypeDistribution();
    }

    @Override
    public double degree( int labelId, int relType, Direction direction )
    {
        return delegate.degree(labelId, relType, direction);
    }

    @Override
    public double liveNodesRatio()
    {
        return delegate.liveNodesRatio();
    }

    @Override
    public long maxAddressableNodes()
    {
        return delegate.maxAddressableNodes();
    }
}
