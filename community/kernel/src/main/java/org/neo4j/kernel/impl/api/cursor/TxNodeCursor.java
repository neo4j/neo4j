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
package org.neo4j.kernel.impl.api.cursor;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.LabelCursor;
import org.neo4j.kernel.api.cursor.NodeCursor;
import org.neo4j.kernel.api.cursor.PropertyCursor;
import org.neo4j.kernel.api.cursor.RelationshipCursor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Overlays transaction state on a {@link NodeCursor}.
 */
public class TxNodeCursor
        implements NodeCursor
{
    private final KernelStatement statement;
    private InstanceCache<TxNodeCursor> cache;
    private NodeCursor nodeCursor;

    public TxNodeCursor( KernelStatement statement, InstanceCache<TxNodeCursor> cache )
    {
        this.statement = statement;
        this.cache = cache;
    }

    public TxNodeCursor init(NodeCursor nodeCursor)
    {
        this.nodeCursor = nodeCursor;
        return this;
    }

    @Override
    public boolean next()
    {
        return nodeCursor.next();
    }

    @Override
    public void close()
    {
        nodeCursor.close();
        nodeCursor = null;
        cache.release( this );
    }

    @Override
    public long getId()
    {
        return nodeCursor.getId();
    }

    @Override
    public LabelCursor labels()
    {
        TxLabelCursor cursor =
                statement.acquireLabelCursor();
        cursor.init( nodeCursor.labels() );
        return cursor;
    }

    @Override
    public PropertyCursor properties()
    {
        TxPropertyCursor cursor = statement.acquireNodePropertyCursor();
        cursor.init(nodeCursor.properties());
        return cursor;
    }

    @Override
    public RelationshipCursor relationships( Direction direction, int... relTypes )
    {
        TxRelationshipCursor cursor = statement.acquireRelationshipCursor();
        cursor.init(nodeCursor.relationships( direction, relTypes));
        return cursor;
    }

    @Override
    public RelationshipCursor relationships( Direction direction )
    {
        TxRelationshipCursor cursor = statement.acquireRelationshipCursor();
        cursor.init( nodeCursor.relationships( direction ) );
        return cursor;
    }
}
