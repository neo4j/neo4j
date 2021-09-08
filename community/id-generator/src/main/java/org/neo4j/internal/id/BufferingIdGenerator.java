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
package org.neo4j.internal.id;

import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.util.List;

import org.neo4j.io.pagecache.context.CursorContext;

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;

class BufferingIdGenerator extends IdGenerator.Delegate
{
    private MutableLongList bufferedDeletedIds;

    BufferingIdGenerator( IdGenerator delegate )
    {
        super( delegate );
        newFreeBuffer();
    }

    private void newFreeBuffer()
    {
        bufferedDeletedIds = LongLists.mutable.empty();
    }

    @Override
    public Marker marker( CursorContext cursorContext )
    {
        Marker actual = super.marker( cursorContext );
        return new Marker()
        {
            @Override
            public void markUsed( long id, int numberOfIds )
            {
                // Goes straight in
                actual.markUsed( id, numberOfIds );
            }

            @Override
            public void markDeleted( long id, int numberOfIds )
            {
                // Run these by the buffering too
                actual.markDeleted( id, numberOfIds );
                synchronized ( BufferingIdGenerator.this )
                {
                    bufferedDeletedIds.add( combinedIdAndNumberOfIds( id, numberOfIds, false ) );
                }
            }

            @Override
            public void markFree( long id, int numberOfIds )
            {
                actual.markFree( id, numberOfIds );
            }

            @Override
            public void close()
            {
                actual.close();
            }
        };
    }

    synchronized void collectBufferedIds( List<BufferingIdGeneratorFactory.IdBuffer> idBuffers )
    {
        if ( !bufferedDeletedIds.isEmpty() )
        {
            idBuffers.add( new BufferingIdGeneratorFactory.IdBuffer( delegate, bufferedDeletedIds ) );
            newFreeBuffer();
        }
    }
}
