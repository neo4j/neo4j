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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.kernel.impl.cache.Cache;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.state.RelationshipChainLoader;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RelationshipLoaderTest
{
    @Test
    public void shouldPopulateCacheIfLoadedRelIsNotInCache() throws Exception
    {
        // Given
        long fromDiskRelId = 12l;

        Cache relCache = mock( Cache.class );

        NodeImpl node = new NodeImpl( 1337l );

        Map<RelIdArray.DirectionWrapper, Iterable<RelationshipRecord>> relsFromDisk = new HashMap<>();
        relsFromDisk.put( RelIdArray.DirectionWrapper.OUTGOING, asList( new RelationshipRecord( fromDiskRelId ) ));
        relsFromDisk.put( RelIdArray.DirectionWrapper.INCOMING, Collections.<RelationshipRecord>emptyList() );

        Pair<Map<DirectionWrapper, Iterable<RelationshipRecord>>, RelationshipLoadingPosition> moreRelationships =
                Pair.<Map<DirectionWrapper, Iterable<RelationshipRecord>>,RelationshipLoadingPosition>of(
                        relsFromDisk, new SingleChainPosition( 1 ) );

        RelationshipChainLoader chainLoader = mock( RelationshipChainLoader.class );
        when( chainLoader.getMoreRelationships( eq( 1337l ), any( RelationshipLoadingPosition.class ),
                any( DirectionWrapper.class ),any( int[].class ) ) ).thenReturn( moreRelationships );
        RelationshipLoader loader = new RelationshipLoader( relCache, chainLoader );

        // When
        Triplet<ArrayMap<Integer,RelIdArray>,List<RelationshipImpl>,RelationshipLoadingPosition> result =
                loader.getMoreRelationships( node, DirectionWrapper.BOTH, new int[0] );

        // Then
        List<RelationshipImpl> relsThatWereNotInCache = result.second();
        assertThat(relsThatWereNotInCache.size(), equalTo(1));
        assertThat(relsThatWereNotInCache.get(0).getId(), equalTo(fromDiskRelId));
    }
}
