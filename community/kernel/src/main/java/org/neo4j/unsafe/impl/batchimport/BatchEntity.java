/**
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;

/**
 * Partly built entity, such as a node or relationship, which moves through processing pipelines during
 * {@link ParallelBatchImporter batch import}.
 */
public class BatchEntity<RECORD extends PrimitiveRecord,INPUT extends InputEntity>
{
    private static final PropertyBlock[] NO_PROPERTY_BLOCKS = new PropertyBlock[0];

    private final RECORD record;
    private final INPUT input;
    private PropertyBlock[] propertyBlocks = NO_PROPERTY_BLOCKS;

    public BatchEntity( RECORD record, INPUT input )
    {
        this.record = record;
        this.input = input;
    }

    public RECORD record()
    {
        return record;
    }

    public INPUT input()
    {
        return input;
    }

    public void setPropertyBlocks( PropertyBlock[] propertyBlocks )
    {
        this.propertyBlocks = propertyBlocks;
    }

    public PropertyBlock[] getPropertyBlocks()
    {
        return propertyBlocks;
    }
}
