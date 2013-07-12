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
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.TokenRecord;

abstract class TokenRecordCheck<RECORD extends TokenRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        implements RecordCheck<RECORD, REPORT>, ComparativeRecordChecker<RECORD, DynamicRecord, REPORT>
{
    @Override
    public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, DiffRecordAccess records )
    {
        check( newRecord, report, records );
    }

    @Override
    public void check( RECORD record, REPORT report, RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        if ( !Record.NO_NEXT_BLOCK.is( record.getNameId() ) )
        {
            report.forReference( name( records, record.getNameId() ), this );
        }
    }

    @Override
    public void checkReference( RECORD record, DynamicRecord name, REPORT report, RecordAccess records )
    {
        if ( !name.inUse() )
        {
            nameNotInUse( report, name );
        }
        else
        {
            if ( name.getLength() <= 0 )
            {
                emptyName( report, name );
            }
        }
    }

    abstract RecordReference<DynamicRecord> name( RecordAccess records, int id );

    abstract void nameNotInUse( REPORT report, DynamicRecord name );

    abstract void emptyName( REPORT report, DynamicRecord name );
}
