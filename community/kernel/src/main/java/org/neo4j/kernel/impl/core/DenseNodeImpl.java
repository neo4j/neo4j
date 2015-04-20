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
package org.neo4j.kernel.impl.core;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelationshipFilter;

import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class DenseNodeImpl extends NodeImpl
{
    public DenseNodeImpl( long id )
    {
        super( id );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, int type,
            CacheUpdateListener cacheUpdateListener )
    {
        return getDegree( relationshipLoader, type, Direction.BOTH, cacheUpdateListener );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        return relationshipLoader.getRelationshipCount( getId(), -1, RelIdArray.wrap( direction ) );
    }

    @Override
    public int getDegree( RelationshipLoader relationshipLoader, int type, Direction direction,
            CacheUpdateListener cacheUpdateListener )
    {
        return relationshipLoader.getRelationshipCount( getId(), type, RelIdArray.wrap( direction ) );
    }

    @Override
    public Iterator<Integer> getRelationshipTypes( RelationshipLoader relationshipLoader,
            CacheUpdateListener cacheUpdateListener )
    {
        return hasMoreRelationshipsToLoad() ? iterator( relationshipLoader.getRelationshipTypes( getId() ) ) :
            super.getRelationshipTypes( relationshipLoader, cacheUpdateListener );
    }

    @Override
    public void visitDegrees( RelationshipLoader relationshipLoader, DegreeVisitor visitor,
                              CacheUpdateListener cacheUpdateListener )
    {
        relationshipLoader.visitRelationshipCounts( getId(), visitor );
    }

    @Override
    protected boolean isDense()
    {
        return true;
    }

    @Override
    protected RelationshipFilter filterForAddingRelationships( final FirstRelationshipIds firstRelationshipIdsToCommit,
            final RelationshipLoadingPosition relChainPosition )
    {
        // Filtering for dense nodes is different from that of sparse nodes in that on-disk and cached
        // are split up the same way. So here we don't need to collect all first ids that sparse nodes will
        // have to do, instead we can check the specific chain each time.
        return new RelationshipFilter()
        {
            @Override
            public boolean accept( int type, DirectionWrapper direction, long firstCachedId )
            {
                long firstIdToCommit = firstRelationshipIdsToCommit.firstIdOf( type, direction );
                assert firstIdToCommit != Record.NO_NEXT_RELATIONSHIP.intValue() :
                        "About to add relationships of " + type + " " + direction +
                        " to node " + getId() + ", but apparently the tx state says that no such relationships " +
                        "are to be added " + firstRelationshipIdsToCommit;

                return  firstCachedId == Record.NO_NEXT_RELATIONSHIP.intValue()

                        // This condition guards for the case where, for a particular relationship chain, there
                        // are no cached relationships yet. This might be because there have been no relationships
                        // loaded or if there simply are no relationships in that chain. For the former,
                        // if the chain position is currently pointing to the same id as we're about to commit
                        // as first id for this chain, then we can tell that a READER has already seen this
                        // change, and so we will not add these relationships.
                        ? relationshipChainAtPosition( relChainPosition, direction, type, firstIdToCommit )

                        // This condition guards for the case where, for a particular relationship chain, there
                        // are one or more cached relationships, and the first relationship loaded and cached
                        // for this chain is the same as the id we're about to commit as first id for this chain,
                        // then we can tell that a READER has already seen this change, and so we will not add
                        // these relationships.
                        : firstCachedId != firstIdToCommit;
            }

            private boolean relationshipChainAtPosition( RelationshipLoadingPosition relChainPosition,
                    DirectionWrapper direction, int type, long firstIdToCommit )
            {
                return relChainPosition != null && !relChainPosition.atPosition( direction, type, firstIdToCommit );
            }
        };
    }
}
