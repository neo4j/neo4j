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
package org.neo4j.kernel.impl.api.store;

import java.util.NoSuchElementException;

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

/**
 * Convert a {@link RelationshipItem} cursor into a {@link RelationshipIterator} that implements {@link Resource).
 */
public class CursorRelationshipIterator implements RelationshipIterator, Resource
{
    private Cursor<RelationshipItem> cursor;
    private boolean hasNext;

    private long id;
    private int type;
    private long startNode;
    private long endNode;

    public CursorRelationshipIterator( Cursor<RelationshipItem> resourceCursor )
    {
        cursor = resourceCursor;
        hasNext = nextCursor();
    }

    private boolean nextCursor()
    {
        if ( cursor != null )
        {
            boolean hasNext = cursor.next();
            if ( !hasNext )
            {
                close();
            }
            return hasNext;
        }
        return false;
    }

    @Override
    public boolean hasNext()
    {
        return hasNext;
    }

    @Override
    public long next()
    {
        if ( hasNext )
        {
            try
            {
                // Copy is necessary here as the nextCursor() in finally will change contents of it
                RelationshipItem item = cursor.get();
                id = item.id();
                type = item.type();
                startNode = item.startNode();
                endNode = item.endNode();

                return item.id();
            }
            finally
            {
                hasNext = nextCursor();
            }
        }
        throw new NoSuchElementException();
    }

    @Override
    public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
            RelationshipVisitor<EXCEPTION> visitor ) throws EXCEPTION
    {
        visitor.visit( id, type, startNode, endNode );
        return false;
    }


    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }
    }
}
