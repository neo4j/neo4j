/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.batchimport.input;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;

import org.neo4j.common.EntityType;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class LenientStoreInputChunkTest
{
    @Test
    void shouldHandleCircularPropertyChain()
    {
        // given
        PropertyStore propertyStore = mock( PropertyStore.class );
        when( propertyStore.newRecord() ).thenAnswer( invocationOnMock -> new PropertyRecord( -1 ) );
        MutableLongObjectMap<PropertyRecord> propertyRecords = LongObjectMaps.mutable.empty();
        long[] propertyRecordIds = new long[]{12, 13, 14, 15};
        for ( int i = 0; i < propertyRecordIds.length; i++ )
        {
            long prev = i == 0 ? NULL_REFERENCE.longValue() : propertyRecordIds[i - 1];
            long id = propertyRecordIds[i];
            long next = i == propertyRecordIds.length - 1 ? propertyRecordIds[1] : propertyRecordIds[i + 1];
            propertyRecords.put( id, new PropertyRecord( id ).initialize( true, prev, next ) );
        }
        doAnswer( invocationOnMock ->
        {
            long id = invocationOnMock.getArgument( 0 );
            PropertyRecord sourceRecord = propertyRecords.get( id );
            PropertyRecord targetRecord = invocationOnMock.getArgument( 1 );
            targetRecord.setId( id );
            targetRecord.initialize( true, sourceRecord.getPrevProp(), sourceRecord.getNextProp() );
            return null;
        } ).when( propertyStore ).getRecordByCursor( anyLong(), any(), any(), any() );

        ReadBehaviour readBehaviour = mock( ReadBehaviour.class );
        LenientStoreInputChunk chunk = new LenientStoreInputChunk( readBehaviour, propertyStore, mock( TokenHolders.class ),
                PageCacheTracer.NULL, StoreCursors.NULL, mock( PageCursor.class ) )
        {
            @Override
            void readAndVisit( long id, InputEntityVisitor visitor, StoreCursors storeCursors )
            {
            }

            @Override
            String recordType()
            {
                return "test";
            }

            @Override
            boolean shouldIncludeProperty( ReadBehaviour readBehaviour, String key, String[] owningEntityTokens )
            {
                return true;
            }
        };

        // when
        NodeRecord primitiveRecord = new NodeRecord( 9 );
        primitiveRecord.initialize( true, propertyRecordIds[0], false, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue() );
        InputEntityVisitor visitor = mock( InputEntityVisitor.class );
        chunk.visitPropertyChainNoThrow( visitor, primitiveRecord, EntityType.NODE, new String[0] );

        // then
        verify( readBehaviour ).error( argThat( format -> format.contains( "circular property chain" ) ), any() );
    }
}
