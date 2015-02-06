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

public class Batch<INPUT,RECORD extends PrimitiveRecord>
{
    public final INPUT[] input;
    public RECORD[] records;
    public final int[] propertyBlocksLengths;
    // This is a special succer. All property blocks for ALL records in this batch sits in this
    // single array. The number of property blocks for a given record sits in propertyBlocksLengths
    // using the same index as the record. So it's a collective size suitable for complete looping
    // over the batch.
    public PropertyBlock[] propertyBlocks;
    // Used by relationship staged to query idMapper and store ids here
    public long[] ids;

    public Batch( INPUT[] input )
    {
        this.input = input;
        this.propertyBlocksLengths = new int[input.length];
    }
}
