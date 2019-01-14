/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency.store.synthetic;

import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Synthetic record type that stands in for a real record to fit in conveniently
 * with consistency checking
 */
public class CountsEntry extends AbstractBaseRecord
{
    private CountsKey key;
    private long count;

    public CountsEntry( CountsKey key, long count )
    {
        super( -1 );
        this.key = key;
        this.count = count;
        setInUse( true );
    }

    @Override
    public void clear()
    {
        super.clear();
        key = null;
        count = 0;
    }

    @Override
    public String toString()
    {
        return "CountsEntry[" + key + ": " + count + "]";
    }

    @Override
    public long getId()
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

    public abstract static class CheckAdapter implements RecordCheck<CountsEntry,ConsistencyReport.CountsConsistencyReport>
    {
    }
}
