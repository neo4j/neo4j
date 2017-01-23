/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.Token;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.unsafe.impl.batchimport.Configuration.DEFAULT;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerators.fromInput;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMappers.actual;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

public class NodeEncoderStepTest
{
    private final StageControl control = mock( StageControl.class );
    private final TokenStore<LabelTokenRecord,Token> tokenStore = mock( TokenStore.class );
    private final BatchingLabelTokenRepository tokenRepository = new BatchingLabelTokenRepository( tokenStore );
    private final NodeStore nodeStore = mock( NodeStore.class );
    private final CapturingSender sender = new CapturingSender();

    @Test
    public void shouldNotAssignLabelsForNodesWithJustLabelField() throws Exception
    {
        // GIVEN
        NodeEncoderStep step = new NodeEncoderStep( control, DEFAULT, actual(), fromInput(), tokenRepository,
                nodeStore, mock( StatsProvider.class ) );

        // WHEN
        InputNode node = new InputNode( "source", 0, 0, 0L, NO_PROPERTIES, null, null, 1L );
        Batch<InputNode,NodeRecord> batchBefore = new Batch<>( new InputNode[] {node} );
        step.process( batchBefore, sender );

        // THEN
        @SuppressWarnings( "unchecked" )
        Batch<InputNode,NodeRecord> batchAfter = (Batch<InputNode,NodeRecord>) single( sender );
        assertNull( batchAfter.labels[0] );
    }

    @Test
    public void shouldNotAssignLabelsForNodesWithNoLabels() throws Exception
    {
        // GIVEN
        NodeEncoderStep step = new NodeEncoderStep( control, DEFAULT, actual(), fromInput(), tokenRepository,
                nodeStore, mock( StatsProvider.class ) );

        // WHEN
        InputNode node = new InputNode( "source", 0, 0, 0L, NO_PROPERTIES, null, NO_LABELS, null );
        Batch<InputNode,NodeRecord> batchBefore = new Batch<>( new InputNode[] {node} );
        step.process( batchBefore, sender );

        // THEN
        @SuppressWarnings( "unchecked" )
        Batch<InputNode,NodeRecord> batchAfter = (Batch<InputNode,NodeRecord>) single( sender );
        assertNull( batchAfter.labels[0] );
    }

    @Test
    public void shouldAssignLabelsForNodesWithLabels() throws Exception
    {
        // GIVEN
        NodeEncoderStep step = new NodeEncoderStep( control, DEFAULT, actual(), fromInput(), tokenRepository,
                nodeStore, mock( StatsProvider.class ) );

        // WHEN
        InputNode node = new InputNode( "source", 0, 0, 0L, NO_PROPERTIES, null, new String[] {"one", "two"}, null );
        Batch<InputNode,NodeRecord> batchBefore = new Batch<>( new InputNode[] {node} );
        step.process( batchBefore, sender );

        // THEN
        @SuppressWarnings( "unchecked" )
        Batch<InputNode,NodeRecord> batchAfter = (Batch<InputNode,NodeRecord>) single( sender );
        assertNotNull( batchAfter.labels[0] );
        assertEquals( 2, batchAfter.labels[0].length );
    }
}
