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
package org.neo4j.unsafe.batchinsert;

import java.util.Iterator;

import org.neo4j.function.IntPredicates;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.RelationshipVisitor;
import org.neo4j.kernel.impl.api.StoreRelationshipIterable;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.store.NeoStore;

import static org.neo4j.graphdb.Direction.BOTH;

abstract class BatchRelationshipIterable<T> implements Iterable<T>, RelationshipVisitor<RuntimeException>
{
    protected final StoreRelationshipIterable storeIterable;

    public BatchRelationshipIterable( NeoStore neoStore, long nodeId )
    {
        try
        {
            this.storeIterable = new StoreRelationshipIterable( neoStore, nodeId, IntPredicates.alwaysTrue(), BOTH );
        }
        catch ( EntityNotFoundException e )
        {
            throw new NotFoundException( e.entityType() + " " + e.entityId() + " not found" );
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return new PrefetchingIterator<T>()
        {
            private final RelationshipIterator storeIterator = storeIterable.iterator();

            @Override
            protected T fetchNextOrNull()
            {
                if ( !storeIterator.hasNext() )
                {
                    return null;
                }
                long relationshipId = storeIterator.next();
                return nextFrom( relationshipId, storeIterator );
            }
        };
    }

    @Override
    public void visit( long relId, int type, long startNode, long endNode ) throws RuntimeException
    {
        throw new UnsupportedOperationException( "Should have been implemented by subclass using it" );
    }

    protected abstract T nextFrom( long relationshipId, RelationshipIterator storeIterator );
}
