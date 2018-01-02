/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.staging.Step;

/**
 * Batch object flowing through several {@link Stage stages} in a {@link ParallelBatchImporter batch import}.
 * Typically each {@link Step} populates or manipulates certain fields and passes the same {@link Batch} instance
 * downstream.
 */
public class Batch<INPUT,RECORD extends PrimitiveRecord>
{
    /**
     * Used in a scenario where a step merely needs to signal that the next step in the stage should execute,
     * not necessarily that it needs any data from the previous step.
     */
    public static final Batch EMPTY = new Batch<>( null );

    public final INPUT[] input;
    public RECORD[] records;
    public int[] propertyBlocksLengths;
    // This is a special succer. All property blocks for ALL records in this batch sits in this
    // single array. The number of property blocks for a given record sits in propertyBlocksLengths
    // using the same index as the record. So it's a collective size suitable for complete looping
    // over the batch.
    public PropertyBlock[] propertyBlocks;
    // Used by ParallelizeByNodeIdStep to help determine any two batches have any id in common
    public long[] sortedIds;
    // Used by relationship staged to query idMapper and store ids here
    public long[] ids;
    public boolean parallelizableWithPrevious;
    public long firstRecordId;
    public long[][] labels;

    public Batch( INPUT[] input )
    {
        this.input = input;
    }
}
