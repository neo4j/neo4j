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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.SimpleLongLayout;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class BlockEntryTest
{
    @Inject
    RandomRule rnd;

    private static final PageCursor pageCursor = ByteArrayPageCursor.wrap( 1000 );
    private static SimpleLongLayout layout;

    @BeforeEach
    void setup()
    {
        layout = SimpleLongLayout.longLayout()
                .withFixedSize( rnd.nextBoolean() )
                .withKeyPadding( rnd.nextInt( 10 ) )
                .build();
    }

    @Test
    void shouldReadWriteSingleEntry()
    {
        // given
        MutableLong writeKey = layout.key( rnd.nextLong() );
        MutableLong writeValue = layout.value( rnd.nextLong() );
        int offset = pageCursor.getOffset();
        BlockEntry.write( pageCursor, layout, writeKey, writeValue );

        // when
        MutableLong readKey = layout.newKey();
        MutableLong readValue = layout.newValue();
        pageCursor.setOffset( offset );
        BlockEntry.read( pageCursor, layout, readKey, readValue );

        // then
        assertEquals( 0, layout.compare( writeKey, readKey ) );
        assertEquals( 0, layout.compare( writeValue, readValue ) );
    }

    @Test
    void shouldReadWriteMultipleEntries()
    {
        List<BlockEntry<MutableLong,MutableLong>> expectedEntries = new ArrayList<>();
        int nbrOfEntries = 10;
        int offset = pageCursor.getOffset();
        for ( int i = 0; i < nbrOfEntries; i++ )
        {
            BlockEntry<MutableLong,MutableLong> entry = new BlockEntry<>( layout.key( rnd.nextLong() ), layout.value( rnd.nextLong() ) );
            BlockEntry.write( pageCursor, layout, entry );
            expectedEntries.add( entry );
        }

        pageCursor.setOffset( offset );
        for ( BlockEntry<MutableLong,MutableLong> expectedEntry : expectedEntries )
        {
            BlockEntry<MutableLong,MutableLong> actualEntry = BlockEntry.read( pageCursor, layout );
            assertBlockEquals( expectedEntry, actualEntry );
        }
    }

    private static void assertBlockEquals( BlockEntry<MutableLong,MutableLong> expected, BlockEntry<MutableLong,MutableLong> actual )
    {
        assertEquals( 0, layout.compare( expected.key(), actual.key() ) );
        assertEquals( 0, layout.compare( expected.value(), actual.value() ) );
    }
}
