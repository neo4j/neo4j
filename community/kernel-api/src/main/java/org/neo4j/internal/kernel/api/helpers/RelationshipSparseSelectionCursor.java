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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.KernelReadTracer;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

/**
 * Helper cursor for traversing specific types and directions of a dense node.
 */
public final class RelationshipSparseSelectionCursor extends RelationshipSparseSelection implements RelationshipSelectionCursor
{
    @Override
    public boolean next()
    {
        if ( !fetchNext() )
        {
            close();
            return false;
        }
        return true;
    }

    @Override
    public void close()
    {
        closeInternal();
        final var listener = closeListener;
        if ( listener != null )
        {
            listener.onClosed( this );
        }
    }

    @Override
    public long relationshipReference()
    {
        return cursor.relationshipReference();
    }

    @Override
    public int type()
    {
        return cursor.type();
    }

    @Override
    public long otherNodeReference()
    {
        return cursor.originNodeReference() == cursor.sourceNodeReference() ?
               cursor.targetNodeReference() : cursor.sourceNodeReference();
    }

    @Override
    public long sourceNodeReference()
    {
        return cursor.sourceNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return cursor.targetNodeReference();
    }

    @Override
    public long propertiesReference()
    {
        return cursor.propertiesReference();
    }

    @Override
    public void properties( PropertyCursor propertyCursor )
    {
        cursor.properties( propertyCursor );
    }

    @Override
    public boolean isClosed()
    {
        return cursor == null || cursor.isClosed();
    }

    @Override
    public void setTracer( KernelReadTracer tracer )
    {
        cursor.setTracer( tracer );
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "RelationshipSparseSelectionCursor[closed state]";
        }
        else
        {
            return "RelationshipSparseSelectionCursor[cursor=" + cursor.toString() + "]";
        }
    }

    public RelationshipTraversalCursor traversalCursor()
    {
        return cursor;
    }
}
