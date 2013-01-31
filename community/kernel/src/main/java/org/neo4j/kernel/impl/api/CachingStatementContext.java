/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Set;

import org.neo4j.kernel.api.StatementContext;

// Only intercepts reading
public class CachingStatementContext extends DelegatingStatementContext
{
    private final PersistenceCache cache;

    public CachingStatementContext( StatementContext actual, PersistenceCache cache )
    {
        super( actual );
        this.cache = cache;
    }
    
    @Override
    public boolean addLabelToNode( long labelId, long nodeId )
    {
        Set<Long> cachedLabels = cache.getLabels( nodeId );
        if ( cachedLabels != null && cachedLabels.contains( labelId ) )
            return false;
        return delegate.addLabelToNode( labelId, nodeId );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        Set<Long> labels = cache.getLabels( nodeId );
        if ( labels != null )
            return labels.contains( labelId );
        return delegate.isLabelSetOnNode( labelId, nodeId );
    }
    
    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        Set<Long> labels = cache.getLabels( nodeId );
        if ( labels != null )
            return labels;
        return delegate.getLabelsForNode( nodeId );
    }
    
    @Override
    public boolean removeLabelFromNode( long labelId, long nodeId )
    {
        Set<Long> cachedLabels = cache.getLabels( nodeId );
        if ( cachedLabels != null && !cachedLabels.contains( labelId ) )
            return false;
        return delegate.removeLabelFromNode( labelId, nodeId );
    }
}
