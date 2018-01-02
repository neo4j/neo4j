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
package org.neo4j.legacy.consistency.checking;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

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
                             CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                             DiffRecordAccess records )
    {
        check( newRecord, engine, records );
        if ( oldRecord.inUse() && !Record.NO_NEXT_BLOCK.is( oldRecord.getNextBlock() ) )
        {
            if ( !newRecord.inUse() || oldRecord.getNextBlock() != newRecord.getNextBlock() )
            {
                DynamicRecord next = dereference.changed( records, oldRecord.getNextBlock() );
                if ( next == null )
                {
                    engine.report().nextNotUpdated();
                }
                // TODO: how to check that the owner of 'next' is now a different property record.
                // TODO: implement previous logic? DynamicRecord must change from used to unused or from unused to used
            }
        }
    }

    @Override
    public void check( DynamicRecord record,
                       CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                       RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        if ( record.getLength() == 0 )
        {
            engine.report().emptyBlock();
        }
        else if ( record.getLength() < 0 )
        {
            engine.report().invalidLength();
        }
        if ( !Record.NO_NEXT_BLOCK.is( record.getNextBlock() ) )
        {
            if ( record.getNextBlock() == record.getId() )
            {
                engine.report().selfReferentialNext();
            }
            else
            {
                engine.comparativeCheck( dereference.lookup( records, record.getNextBlock() ), this );
            }
            if ( record.getLength() < blockSize )
            {
                engine.report().recordNotFullReferencesNext();
            }
        }
    }

    @Override
    public void checkReference( DynamicRecord record, DynamicRecord next,
                                CheckerEngine<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> engine,
                                RecordAccess records )
    {
        if ( !next.inUse() )
        {
            engine.report().nextNotInUse( next );
        }
        else
        {
            if ( next.getLength() <= 0 )
            {
                engine.report().emptyNextBlock( next );
            }
        }
    }
}
