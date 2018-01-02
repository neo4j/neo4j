/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.function.IntSupplier;
import org.neo4j.function.ToIntFunction;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.api.store.CursorRelationshipIterator;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.util.Cursors;

/**
 * Represents a single node from a cursor.
 */
public interface NodeItem
        extends EntityItem
{
    ToIntFunction<LabelItem> GET_LABEL = new ToIntFunction<LabelItem>()
    {
        @Override
        public int apply( LabelItem item )
        {
            return item.getAsInt();
        }
    };

    ToIntFunction<IntSupplier> GET_RELATIONSHIP_TYPE = new ToIntFunction<IntSupplier>()
    {
        @Override
        public int apply( IntSupplier item )
        {
            return item.getAsInt();
        }
    };

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

    /**
     * @return label cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<LabelItem> labels();

    /**
     * @param labelId for specific label to find
     * @return label cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<LabelItem> label( int labelId );

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction, int... relTypes );

    /**
     * @return relationship cursor for current node
     * @throws IllegalStateException if no current node is selected
     */
    Cursor<RelationshipItem> relationships( Direction direction );

    Cursor<IntSupplier> relationshipTypes();

    int degree( Direction direction );

    int degree( Direction direction, int relType );

    boolean isDense();

    Cursor<DegreeItem> degrees();

    // Helper methods

    boolean hasLabel( int labelId );

    PrimitiveIntIterator getLabels();

    RelationshipIterator getRelationships( Direction direction, int[] relTypes );

    RelationshipIterator getRelationships( Direction direction );

    PrimitiveIntIterator getRelationshipTypes();
}
