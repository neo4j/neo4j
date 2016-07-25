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

import java.util.function.Consumer;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.storageengine.api.LabelItem;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

/**
 * Cursor over all labels on a node.
 */
public class StoreLabelCursor implements Cursor<LabelItem>, LabelItem
{
    private long[] labels;
    private int index;
    private int currentLabel;

    private final RecordCursor<DynamicRecord> dynamicLabelRecordCursor;
    private final Consumer<StoreLabelCursor> instanceCache;

    public StoreLabelCursor( RecordCursor<DynamicRecord> dynamicLabelRecordCursor,
            Consumer<StoreLabelCursor> instanceCache )
    {
        this.dynamicLabelRecordCursor = dynamicLabelRecordCursor;
        this.instanceCache = instanceCache;
    }

    public StoreLabelCursor init( NodeRecord nodeRecord )
    {
        this.labels = NodeLabelsField.get( nodeRecord, dynamicLabelRecordCursor );
        return this;
    }

    @Override
    public boolean next()
    {
        if ( index < labels.length )
        {
            currentLabel = safeCastLongToInt( labels[index++] );
            return true;
        }
        else
        {
            return false;
        }
    }

    @Override
    public LabelItem get()
    {
        return this;
    }

    @Override
    public int getAsInt()
    {
        return currentLabel;
    }

    @Override
    public void close()
    {
        // this cursor is pooled so it is better to clear it's state here
        clearState();
        instanceCache.accept( this );
    }

    private void clearState()
    {
        labels = null;
        index = 0;
    }
}
