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
package org.neo4j.kernel.impl.api.store;

import org.junit.Test;

import java.util.function.Supplier;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.util.collection.ContinuableArrayCursor;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CursorRelationshipIteratorTest
{
    @Test
    public void shouldLazilyGoToNext()
    {
        // GIVEN
        Cursor<RelationshipItem> cursor = spy( new ContinuableArrayCursor<>( new Supplier<RelationshipItem[]>()
        {
            private boolean first = true;

            @Override
            public RelationshipItem[] get()
            {
                if ( first )
                {
                    first = false;
                    return new RelationshipItem[] {mock( RelationshipItem.class ), mock( RelationshipItem.class )};
                }
                return null;
            }
        } ) );

        try ( CursorRelationshipIterator iterator = new CursorRelationshipIterator( cursor ) )
        {
            verifyZeroInteractions( cursor );

            // WHEN/THEN
            assertTrue( iterator.hasNext() );
            verify( cursor, times( 1 ) ).next();
            iterator.next();
            verify( cursor, times( 1 ) ).next();

            assertTrue( iterator.hasNext() );
            verify( cursor, times( 2 ) ).next();
            iterator.next();
            verify( cursor, times( 2 ) ).next();

            assertFalse( iterator.hasNext() );
            verify( cursor, times( 3 ) ).next();
        }
    }
}
