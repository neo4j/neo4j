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

import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import static org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import static org.neo4j.kernel.impl.nioneo.store.NodeStore.readOwnerFromDynamicLabelsRecord;
import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.fieldDynamicLabelRecordId;

/**
 * Used by {@link org.neo4j.consistency.checking.full.FullCheck} to verify orphanage for node dynamic label records.
 *
 * Actual list of labels is verified from {@link NodeRecordCheck}
 */
public class NodeDynamicLabelOrphanChainStartCheck
        implements RecordCheck<DynamicRecord,DynamicLabelConsistencyReport>,
        ComparativeRecordChecker<DynamicRecord, DynamicRecord, DynamicLabelConsistencyReport>
{

    private static final
    ComparativeRecordChecker<DynamicRecord, NodeRecord, DynamicLabelConsistencyReport> VALID_NODE_RECORD =
            new ComparativeRecordChecker<DynamicRecord, NodeRecord, DynamicLabelConsistencyReport>()
            {
                @Override
                public void checkReference( DynamicRecord record, NodeRecord nodeRecord,
                                            DynamicLabelConsistencyReport report, RecordAccess records )
                {
                    if ( ! nodeRecord.inUse() )
                    {
                        // if this node record is not in use it is not a valid owner
                        report.orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
                    }
                    else
                    {
                        // if this node record is in use but doesn't point to the dynamic label record
                        // that label record has an invalid owner
                        Long dynamicLabelRecordId = fieldDynamicLabelRecordId( nodeRecord.getLabelField() );
                        long recordId = record.getLongId();
                        if ( dynamicLabelRecordId == null || dynamicLabelRecordId.longValue() != recordId )
                        {
                            report.orphanDynamicLabelRecordDueToInvalidOwner( nodeRecord );
                        }
                    }
                }
            };

    @Override
    public void check( DynamicRecord record, DynamicLabelConsistencyReport report, RecordAccess records )
    {
        if ( record.inUse() && record.isStartRecord() )
        {
            Long ownerId = readOwnerFromDynamicLabelsRecord( record );
            if ( null == ownerId )
            {
                // no owner but in use indicates a broken record
                report.orphanDynamicLabelRecord();
            }
            else
            {
                // look at owning node record to verify consistency
                report.forReference( records.node( ownerId ), VALID_NODE_RECORD );
            }
        }
    }

    @Override
    public void checkChange( DynamicRecord oldRecord, DynamicRecord newRecord, DynamicLabelConsistencyReport report,
                             DiffRecordAccess records )
    {
        check( newRecord, report, records );
    }

    @Override
    public void checkReference( DynamicRecord record, DynamicRecord record2, DynamicLabelConsistencyReport report,
                                RecordAccess records )
    {
    }
}
