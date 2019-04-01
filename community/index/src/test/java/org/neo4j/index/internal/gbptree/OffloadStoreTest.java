/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OffloadStoreTest
{
    private static final int PAGE_SIZE = 256;
    private static final int STABLE_GENERATION = 4;
    private static final int UNSTABLE_GENERATION = 5;

    private static final SimpleByteArrayLayout layout = new SimpleByteArrayLayout( false );
    private PageAwareByteArrayCursor cursor;
    private SimpleIdProvider idProvider;
    private PageCursorFactory pcFactory;
    private OffloadIdValidator idValidator;

    @BeforeEach
    void setup()
    {
        cursor = new PageAwareByteArrayCursor( PAGE_SIZE );
        idProvider = new SimpleIdProvider( cursor::duplicate );
        pcFactory = ( id, flags ) -> cursor.duplicate( id );
        idValidator = OffloadIdValidator.ALWAYS_TRUE;
    }

    @Test
    void mustReadKey() throws IOException
    {
        OffloadStore<RawBytes,RawBytes> offloadStore = new OffloadStoreImpl<>( layout, idProvider, pcFactory, idValidator, PAGE_SIZE );

        RawBytes key = layout.newKey();
        key.bytes = new byte[200];
        long offloadId = offloadStore.writeKey( key, STABLE_GENERATION, UNSTABLE_GENERATION );

        RawBytes into = layout.newKey();
        offloadStore.readKey( offloadId, into );

        assertEquals( 0, layout.compare( key, into ) );
    }
}
