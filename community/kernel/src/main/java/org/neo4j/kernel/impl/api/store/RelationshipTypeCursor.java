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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.cursor.Cursor;
import org.neo4j.storageengine.api.RelationshipItem;
import org.neo4j.storageengine.api.RelationshipTypeItem;

import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP_TYPE;

class RelationshipTypeCursor implements Cursor<RelationshipTypeItem>, RelationshipTypeItem
{
    private final PrimitiveIntSet foundTypes;
    private final Cursor<RelationshipItem> relationships;

    private int value = NO_SUCH_RELATIONSHIP_TYPE;

    RelationshipTypeCursor( Cursor<RelationshipItem> relationships )
    {
        this.relationships = relationships;
        foundTypes = Primitive.intSet( 5 );
    }

    @Override
    public boolean next()
    {
        while ( relationships.next() )
        {
            if ( !foundTypes.contains( relationships.get().type() ) )
            {
                foundTypes.add( relationships.get().type() );
                value = relationships.get().type();
                return true;
            }
        }

        value = NO_SUCH_RELATIONSHIP_TYPE;
        return false;
    }

    @Override
    public void close()
    {
        relationships.close();
    }

    @Override
    public RelationshipTypeItem get()
    {
        return this;
    }

    @Override
    public int getAsInt()
    {
        return value;
    }
}
