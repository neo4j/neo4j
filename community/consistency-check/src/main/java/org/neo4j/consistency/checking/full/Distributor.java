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
package org.neo4j.consistency.checking.full;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

/**
 * Responsible for distributing records to different threads, as part of scanning and processing a store.
 */
public class Distributor<R extends AbstractBaseRecord>
{
    protected long recordsPerCPU;
    protected int numThreads;

    public Distributor( long maxCount, int numThreads )
    {
        this.numThreads = numThreads;
        this.recordsPerCPU = (maxCount / numThreads) + 1;
    }

    public int[] whichQ( R record, int[] currentQ )
    {
        if ( currentQ == null )
        {
            currentQ = new int[] {0};
        }
        currentQ[0]++;
        currentQ[0] %= numThreads;
        return currentQ;
    }

    public long getRecordsPerCPU()
    {
        return recordsPerCPU;
    }

    public int getNumThreads()
    {
        return numThreads;
    }

    public class RelationshipDistributor extends Distributor<RelationshipRecord>
    {
        public RelationshipDistributor( long maxCount, int numThreads )
        {
            super( maxCount, numThreads );
        }

        @Override
        public int[] whichQ( RelationshipRecord relationship, int[] currentQ )
        {
            int qIndex1 = (int) (relationship.getFirstNode() / this.recordsPerCPU);
            int qIndex2 = (int) (relationship.getSecondNode() / this.recordsPerCPU);
            return qIndex1 == qIndex2 ? new int[] {qIndex1} : new int[] {qIndex1, qIndex2};
        }
    }
}
