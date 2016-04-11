/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.id.IdGeneratorImpl;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionSupervisors;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class RelationshipLinkbackStageTest
{
    @Test
    public void reservedIdIsSkipped() throws Exception
    {
        long highId = 5;
        RelationshipStore store = StoreWithReservedId.newRelationshipStoreMock( highId );
        RelationshipLinkbackStage stage = new RelationshipLinkbackStage( Configuration.DEFAULT, store, newCache() );

        ExecutionSupervisors.superviseExecution( ExecutionMonitors.invisible(), Configuration.DEFAULT, stage );

        verify( store, never() ).updateRecord( new RelationshipRecord( IdGeneratorImpl.INTEGER_MINUS_ONE ) );
    }

    private static NodeRelationshipCache newCache()
    {
        int denseNodeThreshold = Integer.parseInt( GraphDatabaseSettings.dense_node_threshold.getDefaultValue() );
        return new NodeRelationshipCache( NumberArrayFactory.HEAP, denseNodeThreshold );
    }
}
