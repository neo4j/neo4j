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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.cache.AutoLoadingCache;
import org.neo4j.kernel.impl.core.DenseNodeImpl;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.RelationshipImpl;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

public class CacheLoaders
{
    public static AutoLoadingCache.Loader<NodeImpl> nodeLoader( final NodeStore nodeStore )
    {
        return new AutoLoadingCache.Loader<NodeImpl>()
        {
            @Override
            public NodeImpl loadById( long id )
            {
                NodeRecord record = nodeStore.loadRecord( id, new NodeRecord( id ) );
                if ( record == null )
                {
                    return null;
                }
                return record.isDense() ? new DenseNodeImpl( id ) : new NodeImpl( id );
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
                RelationshipRecord record = new RelationshipRecord( id );
                if ( !relationshipStore.fillRecord( id, record, RecordLoad.CHECK ) )
                {
                    return null;
                }
                return new RelationshipImpl( id, record.getFirstNode(), record.getSecondNode(), record.getType() );
            }
        };
    }

    private CacheLoaders()
    {   // no instances allowed
    }
}
