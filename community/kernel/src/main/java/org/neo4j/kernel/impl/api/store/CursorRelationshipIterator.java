/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.impl.api.RelationshipVisitor;

/**
 * Convert a {@link RelationshipCursor} into a {@link RelationshipIterator} that implements {@link Resource).
 */
public class CursorRelationshipIterator implements RelationshipIterator, Resource
{
    private RelationshipCursor cursor;
    private boolean hasNext;

    private long id;
    private int type;
    private long startNode;
    private long endNode;

    public CursorRelationshipIterator( RelationshipCursor resourceCursor )
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
        else
        {
            return false;
        }
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
                id = cursor.getId();
                type = cursor.getType();
                startNode = cursor.getStartNode();
                endNode = cursor.getEndNode();

                return id;
            }
            finally
            {
                hasNext = nextCursor();
            }
        }
        else
        {
            throw new NoSuchElementException();
        }
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
