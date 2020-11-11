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
package org.neo4j.kernel.impl.coreapi.internal;

import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;

import static org.neo4j.io.IOUtils.closeAllSilently;

public class RelationshipCursorResourceIterator<CURSOR extends RelationshipIndexCursor> extends PrefetchingEntityResourceIterator<Relationship>
{
    private final CURSOR cursor;

    public RelationshipCursorResourceIterator( CURSOR cursor, EntityFactory<Relationship> relationshipFactory )
    {
        super( relationshipFactory );
        this.cursor = cursor;
    }

    @Override
    long fetchNext()
    {
        if ( cursor.next() )
        {
            return cursor.relationshipReference();
        }
        else
        {
            close();
            return NO_ID;
        }
    }

    @Override
    void closeResources()
    {
        closeAllSilently( cursor );
    }
}
