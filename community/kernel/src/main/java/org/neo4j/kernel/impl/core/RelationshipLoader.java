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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.api.DegreeVisitor;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RelationshipChainLoader;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;
import org.neo4j.kernel.impl.util.RelIdArrayWithLoops;

/**
 * Loads relationships from store, instantiating high level cache objects as it goes.
 * Grunt work is done by {@link RelationshipChainLoader}.
 */
public class RelationshipLoader
{
    private final Cache<RelationshipImpl> relationshipCache;
    private final RelationshipChainLoader chainLoader;

    public RelationshipLoader( Cache<RelationshipImpl> relationshipCache,
                               RelationshipChainLoader chainLoader )
    {
        this.relationshipCache = relationshipCache;
        this.chainLoader = chainLoader;
    }

    public Triplet<ArrayMap<Integer, RelIdArray>, List<RelationshipImpl>, RelationshipLoadingPosition>
            getMoreRelationships( NodeImpl node, DirectionWrapper direction, int[] types )
    {
        long nodeId = node.getId();
        RelationshipLoadingPosition position = node.getRelChainPosition();
        Pair<Map<RelIdArray.DirectionWrapper, Iterable<RelationshipRecord>>,RelationshipLoadingPosition> rels =
                chainLoader.getMoreRelationships( nodeId, position, direction, types );
        ArrayMap<Integer, RelIdArray> newRelationshipMap = new ArrayMap<>();

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

    private RelIdArray getOrCreateRelationships( boolean hasLoops, int typeId,
            ArrayMap<Integer, RelIdArray> loadedRelationships )
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
        if (relImpl != null)
        {
            return relImpl;
        }

        RelationshipImpl loadedRelImpl = new RelationshipImpl( relId, rel.getFirstNode(), rel.getSecondNode(),
                rel.getType()  );
        newlyCreatedRelationships.add( loadedRelImpl );
        return loadedRelImpl;
    }

    public void putAllInRelCache( Collection<RelationshipImpl> relationships )
    {
        relationshipCache.putAll( relationships );
    }

    public int getRelationshipCount( long id, int i, DirectionWrapper direction )
    {
        return chainLoader.getRelationshipCount( id, i, direction );
    }

    public void visitRelationshipCounts( long nodeId, DegreeVisitor visitor )
    {
        chainLoader.visitRelationshipCounts( nodeId, visitor );
    }

    public Integer[] getRelationshipTypes( long id )
    {
        return chainLoader.getRelationshipTypes( id );
    }

    public RelationshipLoadingPosition getRelationshipChainPosition( long id )
    {
        return chainLoader.getRelationshipChainPosition( id );
    }
}
