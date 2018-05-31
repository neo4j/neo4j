/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.consistency.checking;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.LabelIdArray;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;

import static org.neo4j.kernel.impl.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.store.DynamicArrayStore.getRightArray;

public class LabelChainWalker<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport> implements
        ComparativeRecordChecker<RECORD, DynamicRecord, REPORT>
{
    private final Validator<RECORD, REPORT> validator;

    private final MutableLongObjectMap<DynamicRecord> recordIds = new LongObjectHashMap<>();
    private final List<DynamicRecord> recordList = new ArrayList<>();
    private boolean allInUse = true;

    public LabelChainWalker( Validator<RECORD, REPORT> validator )
    {
        this.validator = validator;
    }

    @Override
    public void checkReference( RECORD record, DynamicRecord dynamicRecord,
                                CheckerEngine<RECORD, REPORT> engine,
                                RecordAccess records )
    {
        recordIds.put( dynamicRecord.getId(), dynamicRecord );

        if ( dynamicRecord.inUse() )
        {
            recordList.add( dynamicRecord );
        }
        else
        {
            allInUse = false;
            validator.onRecordNotInUse( dynamicRecord, engine );
        }

        long nextBlock = dynamicRecord.getNextBlock();
        if ( Record.NO_NEXT_BLOCK.is( nextBlock ) )
        {
            if ( allInUse )
            {
                // only validate label ids if all dynamic records seen were in use
                validator.onWellFormedChain( labelIds( recordList ), engine, records );
            }
        }
        else
        {
            final DynamicRecord nextRecord = recordIds.get( nextBlock );
            if ( nextRecord != null )
            {
                validator.onRecordChainCycle( nextRecord, engine );
            }
            else
            {
                engine.comparativeCheck( records.nodeLabels( nextBlock ), this );
            }
        }
    }

    public static long[] labelIds( List<DynamicRecord> recordList )
    {
        long[] idArray =
                (long[]) getRightArray( readFullByteArrayFromHeavyRecords( recordList, PropertyType.ARRAY ) ).asObject();
        return LabelIdArray.stripNodeId( idArray );
    }

    public interface Validator<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport>
    {
        void onRecordNotInUse( DynamicRecord dynamicRecord, CheckerEngine<RECORD, REPORT> engine );
        void onRecordChainCycle( DynamicRecord record, CheckerEngine<RECORD, REPORT> engine );
        void onWellFormedChain( long[] labelIds, CheckerEngine<RECORD, REPORT> engine, RecordAccess records );
    }
}
