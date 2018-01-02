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
package org.neo4j.legacy.consistency.store.synthetic;

import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.legacy.consistency.checking.CheckerEngine;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;

/**
 * Synthetic record type that stands in for a real record to fit in conveniently
 * with consistency checking
 */
public class CountsEntry extends AbstractBaseRecord
{
    private final CountsKey key;
    private final long count;

    public CountsEntry( CountsKey key, long count )
    {
        this.key = key;
        this.count = count;
        setInUse( true );
    }

    @Override
    public String toString()
    {
        return "CountsEntry[" + key + ": " + count + "]";
    }

    @Override
    public long getLongId()
    {
        throw new UnsupportedOperationException();
    }

    public CountsKey getCountsKey()
    {
        return key;
    }

    public long getCount()
    {
        return count;
    }

    public static abstract class CheckAdapter implements RecordCheck<CountsEntry,ConsistencyReport.CountsConsistencyReport>
    {
        @Override
        public void checkChange( CountsEntry oldRecord, CountsEntry newRecord,
                                 CheckerEngine<CountsEntry,ConsistencyReport.CountsConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            throw new UnsupportedOperationException();
        }
    }
}
