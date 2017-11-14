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
package org.neo4j.unsafe.impl.batchimport;

import java.util.concurrent.atomic.LongAdder;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Simply counts all written entities and properties and can present totals in the end.
 */
public class CountingStoreUpdateMonitor implements EntityStoreUpdaterStep.Monitor
{
    private final LongAdder nodes = new LongAdder();
    private final LongAdder relationships = new LongAdder();
    private final LongAdder properties = new LongAdder();

    @Override
    public void entitiesWritten( Class<? extends PrimitiveRecord> type, long count )
    {
        if ( type.equals( NodeRecord.class ) )
        {
            nodes.add( count );
        }
        else if ( type.equals( RelationshipRecord.class ) )
        {
            relationships.add( count );
        }
        else
        {
            throw new IllegalArgumentException( type.getName() );
        }
    }

    @Override
    public void propertiesWritten( long count )
    {
        properties.add( count );
    }

    public long propertiesWritten()
    {
        return properties.sum();
    }

    public long nodesWritten()
    {
        return nodes.sum();
    }

    public long relationshipsWritten()
    {
        return relationships.sum();
    }
}
