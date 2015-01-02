/**
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;

public class RelationshipLoader
{
    private final PersistenceManager persistenceManager;
    private final Cache<RelationshipImpl> relationshipCache;

    public RelationshipLoader(PersistenceManager persistenceManager, Cache<RelationshipImpl> relationshipCache )
    {
        this.persistenceManager = persistenceManager;
        this.relationshipCache = relationshipCache;
    }

    public Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, Long> getMoreRelationships( NodeImpl node )
    {
        long nodeId = node.getId();
        long position = node.getRelChainPosition();
        Pair<Map<RelIdArray.DirectionWrapper, Iterable<RelationshipRecord>>, Long> rels =
                persistenceManager.getMoreRelationships( nodeId, position );
        ArrayMap<Integer, RelIdArray> newRelationshipMap =
                new ArrayMap<>();

        List<RelationshipImpl> relsList = new ArrayList<>( 150 );

        Iterable<RelationshipRecord> loops = rels.first().get( RelIdArray.DirectionWrapper.BOTH );
        boolean hasLoops = loops != null;
        if ( hasLoops )
        {
            populateLoadedRelationships( loops, relsList, RelIdArray.DirectionWrapper.BOTH, true, newRelationshipMap );
        }
        populateLoadedRelationships( rels.first().get( RelIdArray.DirectionWrapper.OUTGOING ), relsList,
                RelIdArray.DirectionWrapper.OUTGOING, hasLoops,
                newRelationshipMap
        );
        populateLoadedRelationships( rels.first().get( RelIdArray.DirectionWrapper.INCOMING ), relsList,
                RelIdArray.DirectionWrapper.INCOMING, hasLoops,
                newRelationshipMap
        );

        return Triplet.of( newRelationshipMap, relsList, rels.other() );
    }

    /**
     * @param loadedRelationshipsOutputParameter
     *         This is the return value for this method. It's written like this
     *         because several calls to this method are used to gradually build up
     *         the map of RelIdArrays that are ultimately involved in the operation.
     */
    private void populateLoadedRelationships( Iterable<RelationshipRecord> loadedRelationshipRecords,
                                              List<RelationshipImpl> relsList,
                                              RelIdArray.DirectionWrapper dir,
                                              boolean hasLoops,
                                              ArrayMap<Integer, RelIdArray> loadedRelationshipsOutputParameter )
    {
        for ( RelationshipRecord rel : loadedRelationshipRecords )
        {
            long relId = rel.getId();
            RelationshipImpl relImpl = getOrCreateRelationshipFromCache( relsList, rel, relId );

            getOrCreateRelationships( hasLoops, relImpl.getTypeId(), loadedRelationshipsOutputParameter )
                    .add( relId, dir );
        }
    }

    private RelIdArray getOrCreateRelationships( boolean hasLoops, int typeId, ArrayMap<Integer, RelIdArray> loadedRelationships )
    {
        RelIdArray relIdArray = loadedRelationships.get( typeId );
        if ( relIdArray != null )
        {
            return relIdArray;
        }
        RelIdArray loadedRelIdArray = hasLoops ? new RelIdArrayWithLoops( typeId ) : new RelIdArray( typeId );
        loadedRelationships.put( typeId, loadedRelIdArray );
        return loadedRelIdArray;
    }

    private RelationshipImpl getOrCreateRelationshipFromCache( List<RelationshipImpl> newlyCreatedRelationships,
                                                               RelationshipRecord rel, long relId )
    {
        RelationshipImpl relImpl = relationshipCache.get( relId );
        if (relImpl != null) return relImpl;

        RelationshipImpl loadedRelImpl = new RelationshipImpl( relId, rel.getFirstNode(), rel.getSecondNode(),
                rel.getType(), false );
        newlyCreatedRelationships.add( loadedRelImpl );
        return loadedRelImpl;
    }

}
