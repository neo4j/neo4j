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
package org.neo4j.cypher.internal.codegen;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

/**
 * Utilities for working with cursors from within generated code
 */
public final class CompiledCursorUtils
{
    /**
     * Do not instantiate this class
     */
    private CompiledCursorUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Fetches a given property from a node
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param propertyCursor The property cursor to use
     * @param prop The id of the property to find
     * @return The value of the given property
     * @throws EntityNotFoundException If the node cannot be find.
     */
    public static Value nodeGetProperty( Read read, NodeCursor nodeCursor, long node, PropertyCursor propertyCursor,
            int prop ) throws EntityNotFoundException
    {
        if ( prop == StatementConstants.NO_SUCH_PROPERTY_KEY )
        {
            return Values.NO_VALUE;
        }
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, node );
        }
        nodeCursor.properties( propertyCursor );
        while ( propertyCursor.next() )
        {
            if ( propertyCursor.propertyKey() == prop )
            {
                return propertyCursor.propertyValue();
            }
        }

        return Values.NO_VALUE;
    }

    /**
     * Checks if given node has a given label.
     *
     * @param read The current Read instance
     * @param nodeCursor The node cursor to use
     * @param node The id of the node
     * @param label The id of the label
     * @return <tt>true</tt> if the node has the label, otherwise <tt>false</tt>
     * @throws EntityNotFoundException if the node is not there.
     */
    public static boolean nodeHasLabel( Read read, NodeCursor nodeCursor, long node, int label )
            throws EntityNotFoundException
    {
        if ( label == StatementConstants.NO_SUCH_LABEL )
        {
            return false;
        }
        read.singleNode( node, nodeCursor );
        if ( !nodeCursor.next() )
        {
            throw new EntityNotFoundException( EntityType.NODE, node );
        }

        return nodeCursor.labels().contains( label );
    }
}

