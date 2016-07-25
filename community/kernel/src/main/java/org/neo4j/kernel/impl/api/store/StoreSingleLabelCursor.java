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

import org.neo4j.kernel.impl.store.RecordCursor;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.util.InstanceCache;

/**
 * Cursor over a specific label on a node.
 */
public class StoreSingleLabelCursor extends StoreLabelCursor
{
    private int labelId;

    public StoreSingleLabelCursor( RecordCursor<DynamicRecord> dynamicLabelRecordCursor,
            InstanceCache<StoreSingleLabelCursor> instanceCache )
    {
        super( dynamicLabelRecordCursor, (Consumer) instanceCache );
    }

    public StoreSingleLabelCursor init( NodeRecord nodeRecord, int labelId )
    {
        super.init( nodeRecord );
        this.labelId = labelId;
        return this;
    }

    @Override
    public boolean next()
    {
        while ( super.next() )
        {
            if ( get().getAsInt() == labelId )
            {
                return true;
            }
        }

        return false;
    }
}
