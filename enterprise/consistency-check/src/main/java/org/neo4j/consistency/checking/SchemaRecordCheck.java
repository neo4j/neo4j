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
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;

public class SchemaRecordCheck implements RecordCheck<DynamicRecord, ConsistencyReport.SchemaConsistencyReport>
{
    private final SchemaStore store;
    public SchemaRecordCheck( SchemaStore store )
    {
        this.store = store;
    }

    @Override
    public void check( DynamicRecord record, ConsistencyReport.SchemaConsistencyReport report, RecordAccess records )
    {
        if ( record.inUse() && record.isStartRecord() )
        {
            SchemaRule rule = store.loadSingleSchemaRule( record.getId() );
            report.forReference( records.label( (int) rule.getLabel() ), VALID_LABEL );
        }
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord,
                             ConsistencyReport.SchemaConsistencyReport report, DiffRecordAccess records )
    {
    }

    public static final ComparativeRecordChecker<DynamicRecord,LabelTokenRecord,
            ConsistencyReport.SchemaConsistencyReport> VALID_LABEL =
            new ComparativeRecordChecker<DynamicRecord, LabelTokenRecord, ConsistencyReport.SchemaConsistencyReport>()
    {
        @Override
        public void checkReference( DynamicRecord record, LabelTokenRecord labelTokenRecord,
                                    ConsistencyReport.SchemaConsistencyReport report, RecordAccess records )
        {
            if ( !record.inUse() )
            {
                report.labelNotInUse( labelTokenRecord );
            }
        }
    };
}
