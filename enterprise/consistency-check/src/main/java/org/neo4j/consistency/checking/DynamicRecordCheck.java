/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

class DynamicRecordCheck
        implements RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport>,
        ComparativeRecordChecker<DynamicRecord, DynamicRecord, ConsistencyReport.DynamicConsistencyReport>
{
    private final int blockSize;
    private final DynamicStore dereference;
    private final RecordStore<DynamicRecord> store;

    DynamicRecordCheck( RecordStore<DynamicRecord> store, DynamicStore dereference )
    {
        this.blockSize = store.getRecordSize() - store.getRecordHeaderSize();
        this.dereference = dereference;
        this.store = store;
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                             ConsistencyReport.DynamicConsistencyReport report, DiffRecordAccess records )
    {
        check( newRecord, report, records );
        if ( oldRecord.inUse() && !Record.NO_NEXT_BLOCK.is( oldRecord.getNextBlock() ) )
        {
            if ( !newRecord.inUse() || oldRecord.getNextBlock() != newRecord.getNextBlock() )
            {
                DynamicRecord next = dereference.changed( records, oldRecord.getNextBlock() );
                if ( next == null )
                {
                    report.nextNotUpdated();
                }
                // TODO: how to check that the owner of 'next' is now a different property record.
                // TODO: implement previous logic? DynamicRecord must change from used to unused or from unused to used
            }
        }
    }

    @Override
    public void check( DynamicRecord record, ConsistencyReport.DynamicConsistencyReport report, RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        if ( record.getLength() == 0 )
        {
            report.emptyBlock();
        }
        else if ( record.getLength() < 0 )
        {
            report.invalidLength();
        }
        if ( !Record.NO_NEXT_BLOCK.is( record.getNextBlock() ) )
        {
            if ( record.getNextBlock() == record.getId() )
            {
                report.selfReferentialNext();
            }
            else
            {
                report.forReference( dereference.lookup( records, record.getNextBlock() ), this );
            }
            if ( record.getLength() < blockSize )
            {
                report.recordNotFullReferencesNext();
            }
        }
    }

    @Override
    public void checkReference( DynamicRecord record, DynamicRecord next,
                                ConsistencyReport.DynamicConsistencyReport report, RecordAccess records )
    {
        if ( !next.inUse() )
        {
            report.nextNotInUse( next );
        }
        else
        {
            if ( next.getLength() <= 0 )
            {
                report.emptyNextBlock( next );
            }
        }
    }
}
