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
package org.neo4j.consistency.store;

import org.neo4j.consistency.report.PendingReferenceCheck;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class DirectRecordReference<RECORD extends AbstractBaseRecord> implements RecordReference<RECORD>
{
    final RECORD record;
    final RecordAccess records;

    public DirectRecordReference( RECORD record, RecordAccess records )
    {
        this.record = record;
        this.records = records;
    }

    @Override
    public void dispatch( PendingReferenceCheck<RECORD> reporter )
    {
        reporter.checkReference( record, records );
    }

    public RECORD record()
    {
        return record;
    }
}
