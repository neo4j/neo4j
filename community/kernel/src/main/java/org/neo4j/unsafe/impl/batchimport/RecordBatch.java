/**
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

/**
 * Batch of created records, i.e. entity records with their property records and friends.
 *
 * @param <ENTITY> the type of entities in this batch.
 */
public class RecordBatch<ENTITY extends PrimitiveRecord>
{
    private final List<ENTITY> entityRecords;
    private final Iterable<PropertyRecord> propertyRecords;

    public RecordBatch( List<ENTITY> entityRecords, Iterable<PropertyRecord> propertyRecords )
    {
        this.entityRecords = entityRecords;
        this.propertyRecords = propertyRecords;
    }

    public List<ENTITY> getEntityRecords()
    {
        return entityRecords;
    }

    public Iterable<PropertyRecord> getPropertyRecords()
    {
        return propertyRecords;
    }
}
