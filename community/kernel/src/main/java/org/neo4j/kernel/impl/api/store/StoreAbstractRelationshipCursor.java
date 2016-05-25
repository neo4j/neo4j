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
package org.neo4j.kernel.impl.api.store;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.cursor.EntityItemHelper;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.RecordCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.util.InstanceCache;
import org.neo4j.storageengine.api.PropertyItem;
import org.neo4j.storageengine.api.RelationshipItem;

/**
 * Base cursor for relationships.
 */
public abstract class StoreAbstractRelationshipCursor extends EntityItemHelper
        implements Cursor<RelationshipItem>, RelationshipItem
{
    protected final RelationshipRecord relationshipRecord;
    protected final RecordCursor<RelationshipRecord> relationshipRecordCursor;

    private final InstanceCache<StoreSinglePropertyCursor> singlePropertyCursor;
    private final InstanceCache<StorePropertyCursor> allPropertyCursor;

    public StoreAbstractRelationshipCursor( RelationshipRecord relationshipRecord, RecordCursors cursors )
    {
        this.relationshipRecordCursor = cursors.relationship();
        this.relationshipRecord = relationshipRecord;

        singlePropertyCursor = new InstanceCache<StoreSinglePropertyCursor>()
        {
            @Override
            protected StoreSinglePropertyCursor create()
            {
                return new StoreSinglePropertyCursor( cursors, this );
            }
        };
        allPropertyCursor = new InstanceCache<StorePropertyCursor>()
        {
            @Override
            protected StorePropertyCursor create()
            {
                return new StorePropertyCursor( cursors, this );
            }
        };
    }

    @Override
    public RelationshipItem get()
    {
        return this;
    }

    @Override
    public long id()
    {
        return relationshipRecord.getId();
    }

    @Override
    public int type()
    {
        return relationshipRecord.getType();
    }

    @Override
    public long startNode()
    {
        return relationshipRecord.getFirstNode();
    }

    @Override
    public long endNode()
    {
        return relationshipRecord.getSecondNode();
    }

    @Override
    public long otherNode( long nodeId )
    {
        return relationshipRecord.getFirstNode() == nodeId ?
                relationshipRecord.getSecondNode() : relationshipRecord.getFirstNode();
    }


    @Override
    public Cursor<PropertyItem> properties()
    {
        return allPropertyCursor.get().init( relationshipRecord.getNextProp() );
    }

    @Override
    public Cursor<PropertyItem> property( int propertyKeyId )
    {
        return singlePropertyCursor.get().init( relationshipRecord.getNextProp(), propertyKeyId );
    }
}
