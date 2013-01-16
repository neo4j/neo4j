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
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.kernel.impl.api.TxState.NodeState;
import org.neo4j.kernel.impl.cache.EntityWithSize;
import org.neo4j.kernel.impl.cache.LockStripedCache;
import org.neo4j.kernel.impl.cache.SoftLruCache;

public class PersistenceCache
{
    private final LockStripedCache<CachedNodeEntity> nodeCache;

    public PersistenceCache( LockStripedCache.Loader<CachedNodeEntity> nodeLoader )
    {
        this.nodeCache = new LockStripedCache<CachedNodeEntity>( new SoftLruCache<CachedNodeEntity>( "Kernel API label cache" ),
                32, nodeLoader );
    }
    
    public void apply( TxState state )
    {
        for ( NodeState stateEntity : state.getNodes() )
        {
            CachedNodeEntity entity = nodeCache.getIfCached( stateEntity.getId() );
            if ( entity == null )
            {
                continue;
            }
            
            entity.addLabels( stateEntity.getAddedLabels() );
        }
    }

    public Set<Long> getLabels( long nodeId )
    {
        CachedNodeEntity node = nodeCache.get( nodeId );
        return node != null ? node.getLabels() : null;
    }
    
    public static class CachedNodeEntity implements EntityWithSize
    {
        private final long id;
        private final Set<Long> labels = new CopyOnWriteArraySet<Long>();

        public CachedNodeEntity( long id )
        {
            this.id = id;
        }

        public void addLabels( Set<Long> additionalLabels )
        {
            labels.addAll( additionalLabels );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public void setRegisteredSize( int size )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getRegisteredSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException();
        }

        public Set<Long> getLabels()
        {
            return labels;
        }
    }
}
