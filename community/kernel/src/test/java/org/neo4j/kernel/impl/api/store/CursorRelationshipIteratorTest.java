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
package org.neo4j.kernel.impl.api.store;

import org.junit.Ignore;
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
import static org.neo4j.kernel.impl.locking.LockService.NO_LOCK;

public class CursorRelationshipIteratorTest
{
    @Ignore( "In order to support Iterator Stability isolation level, we actually have to peek the next cursor item " +
             "eagerly" )
    @Test
    public void shouldLazilyGoToNext() throws Exception
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

        try ( CursorRelationshipIterator iterator = new CursorRelationshipIterator( cursor, NO_LOCK ) )
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
