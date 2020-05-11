/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.notAnOffloadNode;
import static org.neo4j.index.internal.gbptree.GBPTreeCorruption.pageSpecificCorruption;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public class GBPTreeConsistencyCheckerDynamicSizeTest extends GBPTreeConsistencyCheckerTestBase<RawBytes,RawBytes>
{
    @Override
    protected TestLayout<RawBytes,RawBytes> getLayout()
    {
        return new SimpleByteArrayLayout( true );
    }

    @Test
    void offloadPointerPointToNonOffloadPage() throws IOException
    {
        try ( GBPTree<RawBytes,RawBytes> index = index().build() )
        {
            int keySize = index.inlineKeyValueSizeCap();
            RawBytes key = key( keySize + 1 );
            RawBytes value = value( 0 );
            try ( Writer<RawBytes,RawBytes> writer = index.writer( NULL ) )
            {
                writer.put( key, value );
            }

            GBPTreeInspection<RawBytes,RawBytes> inspection = inspect( index );
            ImmutableLongList offloadNodes = inspection.getOffloadNodes();
            long offloadNode = offloadNodes.get( random.nextInt( offloadNodes.size() ) );

            index.unsafe( pageSpecificCorruption( offloadNode, notAnOffloadNode() ), NULL );

            assertReportException( index, offloadNode );
        }
    }

    private RawBytes key( int keySize, byte... firstBytes )
    {
        RawBytes key = layout.newKey();
        key.bytes = new byte[keySize];
        for ( int i = 0; i < firstBytes.length && i < keySize; i++ )
        {
            key.bytes[i] = firstBytes[i];
        }
        return key;
    }

    private RawBytes value( int valueSize )
    {
        RawBytes value = layout.newValue();
        value.bytes = new byte[valueSize];
        return value;
    }

    private static <KEY,VALUE> void assertReportException( GBPTree<KEY,VALUE> index, long targetNode ) throws IOException
    {
        MutableBoolean called = new MutableBoolean();
        index.consistencyCheck( new GBPTreeConsistencyCheckVisitor.Adaptor<>()
        {
            @Override
            public void exception( Exception e )
            {
                called.setTrue();
                assertThat( e.getMessage() ).contains( "Tried to read from offload store but page is not an offload page." );
            }
        }, NULL );
        assertCalled( called );
    }
}
