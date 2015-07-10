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

import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.cursor.LabelItem;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.api.cursor.PropertyItem;
import org.neo4j.kernel.api.cursor.RelationshipItem;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Base cursor for nodes.
 */
public abstract class StoreAbstractNodeCursor implements Cursor<NodeItem>, NodeItem
{
    protected final NodeRecord nodeRecord;
    protected final NodeStore nodeStore;
    protected StoreStatement storeStatement;

    public StoreAbstractNodeCursor( NodeRecord nodeRecord, NodeStore nodeStore, StoreStatement storeStatement )
    {
        this.nodeRecord = nodeRecord;
        this.nodeStore = nodeStore;
        this.storeStatement = storeStatement;
    }

    @Override
    public NodeItem get()
    {
        return this;
    }

    @Override
    public long id()
    {
        return nodeRecord.getId();
    }

    @Override
    public Cursor<LabelItem> labels()
    {
        return storeStatement.acquireLabelCursor( parseLabelsField( nodeRecord ).get( nodeStore ) );
    }

    @Override
    public Cursor<LabelItem> label( int labelId )
    {
        return storeStatement.acquireSingleLabelCursor( parseLabelsField( nodeRecord ).get( nodeStore ), labelId );
    }

    @Override
    public Cursor<PropertyItem> properties()
    {
        return storeStatement.acquirePropertyCursor( nodeRecord.getNextProp() );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        return storeStatement.acquireSinglePropertyCursor( nodeRecord.getNextProp(), propertyKeyId );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction )
    {
        return storeStatement.acquireNodeRelationshipCursor( nodeRecord.isDense(), nodeRecord.getNextRel(),
                nodeRecord.getId(), direction, null );
    }

    @Override
    public Cursor<RelationshipItem> relationships( Direction direction, int... relTypes )
    {
        return storeStatement.acquireNodeRelationshipCursor( nodeRecord.isDense(), nodeRecord.getNextRel(),
                nodeRecord.getId(), direction, relTypes );
    }
}
