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
package org.neo4j.kernel.api.cursor;

import java.util.Arrays;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.api.store.CursorRelationshipIterator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.Cursors;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.LabelItem;
import org.neo4j.storageengine.api.NodeItem;

public abstract class NodeItemHelper
        extends EntityItemHelper
        implements NodeItem
{
    @Override
    public boolean hasLabel( int labelId )
    {
        try ( Cursor<LabelItem> labelCursor = label( labelId ) )
        {
            return labelCursor.next();
        }
    }

    @Override
    public PrimitiveIntIterator getLabels()
    {
        return Cursors.intIterator( labels(), GET_LABEL );
    }

    @Override
    public RelationshipIterator getRelationships( Direction direction, int[] relTypes )
    {
        relTypes = deduplicate( relTypes );

        return new CursorRelationshipIterator( relationships( direction, relTypes ) );
    }

    @Override
    public RelationshipIterator getRelationships( Direction direction )
    {
        return new CursorRelationshipIterator( relationships( direction ) );
    }

    @Override
    public PrimitiveIntIterator getRelationshipTypes()
    {
        return Cursors.intIterator( relationshipTypes(), GET_RELATIONSHIP_TYPE );
    }

    private static int[] deduplicate( int[] types )
    {
        int unique = 0;
        for ( int i = 0; i < types.length; i++ )
        {
            int type = types[i];
            for ( int j = 0; j < unique; j++ )
            {
                if ( type == types[j] )
                {
                    type = -1; // signal that this relationship is not unique
                    break; // we will not find more than one conflict
                }
            }
            if ( type != -1 )
            { // this has to be done outside the inner loop, otherwise we'd never accept a single one...
                types[unique++] = types[i];
            }
        }
        if ( unique < types.length )
        {
            types = Arrays.copyOf( types, unique );
        }
        return types;
    }
}
