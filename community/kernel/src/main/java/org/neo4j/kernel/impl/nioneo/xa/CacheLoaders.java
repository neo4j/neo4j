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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.core.DenseNodeImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

public class CacheLoaders
{
    public static AutoLoadingCache.Loader<NodeImpl> nodeLoader( final NodeStore nodeStore )
    {
        return new AutoLoadingCache.Loader<NodeImpl>()
        {
            @Override
            public NodeImpl loadById( long id )
            {
                try
                {
                    NodeRecord record = nodeStore.getRecord( id );
                    return record.isDense() ? new DenseNodeImpl( id ) : new NodeImpl( id );
                }
                catch ( InvalidRecordException e )
                {
                    return null;
                }
            }
        };
    }
    
    public static AutoLoadingCache.Loader<RelationshipImpl> relationshipLoader(
            final RelationshipStore relationshipStore )
    {
        return new AutoLoadingCache.Loader<RelationshipImpl>()
        {
            @Override
            public RelationshipImpl loadById( long id )
            {
                try
                {
                    RelationshipRecord record = relationshipStore.getRecord( id );
                    return new RelationshipImpl( id, record.getFirstNode(), record.getSecondNode(), record.getType() );
                }
                catch ( InvalidRecordException e )
                {
                    return null;
                }
            }
        };
    }
    
    private CacheLoaders()
    {   // no instances allowed
    }
}
