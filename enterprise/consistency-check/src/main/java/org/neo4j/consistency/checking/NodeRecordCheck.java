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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DiffRecordAccess;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;

import static java.util.Arrays.copyOfRange;
import static java.util.Arrays.sort;

import static org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore.readFullByteArrayFromHeavyRecords;
import static org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore.getRightArray;

class NodeRecordCheck extends PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    NodeRecordCheck()
    {
        super( RelationshipField.NEXT_REL, LabelsField.LABELS );
    }

    private enum RelationshipField implements RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>,
            ComparativeRecordChecker<NodeRecord, RelationshipRecord, ConsistencyReport.NodeConsistencyReport>
    {
        NEXT_REL
        {
            @Override
            public void checkConsistency( NodeRecord node, ConsistencyReport.NodeConsistencyReport report,
                                          RecordAccess records )
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.is( node.getNextRel() ) )
                {
                    report.forReference( records.relationship( node.getNextRel() ), this );
                }
            }

            @Override
            public void checkReference( NodeRecord node, RelationshipRecord relationship,
                                        ConsistencyReport.NodeConsistencyReport report, RecordAccess records )
            {
                if ( !relationship.inUse() )
                {
                    report.relationshipNotInUse( relationship );
                }
                else
                {
                    NodeField selectedField = NodeField.select( relationship, node );
                    if ( selectedField == null )
                    {
                        report.relationshipForOtherNode( relationship );
                    }
                    else
                    {
                        NodeField[] fields;
                        if ( relationship.getFirstNode() == relationship.getSecondNode() )
                        { // this relationship is a loop, report both inconsistencies
                            fields = NodeField.values();
                        }
                        else
                        {
                            fields = new NodeField[]{selectedField};
                        }
                        for ( NodeField field : fields )
                        {
                            if ( !Record.NO_NEXT_RELATIONSHIP.is( field.prev( relationship ) ) )
                            {
                                field.notFirstInChain( report, relationship );
                            }
                        }
                    }
                }
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     ConsistencyReport.NodeConsistencyReport report,
                                     DiffRecordAccess records )
            {
                if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
                {
                    if ( !Record.NO_NEXT_RELATIONSHIP.is( valueFrom( oldRecord ) )
                         && records.changedRelationship( valueFrom( oldRecord ) ) == null )
                    {
                        report.relationshipNotUpdated();
                    }
                }
            }

            @Override
            public long valueFrom( NodeRecord record )
            {
                return record.getNextRel();
            }
        }
    }

    private enum LabelsField implements RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>,
            ComparativeRecordChecker<NodeRecord, LabelTokenRecord, ConsistencyReport.NodeConsistencyReport>
    {
        LABELS
        {
            @Override
            public void checkConsistency( NodeRecord node, ConsistencyReport.NodeConsistencyReport report,
                                          RecordAccess records )
            {
                NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );
                if ( nodeLabels instanceof DynamicNodeLabels )
                {
                    DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
                    long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
                    RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
                    report.forReference( firstRecordReference, new NodeLabelsComparativeRecordChecker() );
                }
                else
                {
                    validateLabelIds( nodeLabels.get( null ), report, records );
                }
            }

            private void validateLabelIds( long[] labelIds, ConsistencyReport.NodeConsistencyReport report,
                                           RecordAccess records )
            {
                for ( long labelId : labelIds )
                {
                    report.forReference( records.label( (int) labelId ), this );
                }
                sort( labelIds );
                for ( int i = 1; i < labelIds.length; i++ )
                {
                    if ( labelIds[i - 1] == labelIds[i] )
                    {
                        report.labelDuplicate( labelIds[i] );
                    }
                }
            }

            @Override
            public void checkReference( NodeRecord node, LabelTokenRecord labelTokenRecord,
                                        ConsistencyReport.NodeConsistencyReport report, RecordAccess records )
            {
                if ( !labelTokenRecord.inUse() )
                {
                    report.labelNotInUse( labelTokenRecord );
                }
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     ConsistencyReport.NodeConsistencyReport report,
                                     DiffRecordAccess records )
            {
                // nothing to check: no back references from labels to nodes
            }

            @Override
            public long valueFrom( NodeRecord record )
            {
                return record.getLabelField();
            }

            class NodeLabelsComparativeRecordChecker implements
                    ComparativeRecordChecker<NodeRecord, DynamicRecord, ConsistencyReport.NodeConsistencyReport>
            {
                private HashMap<Long, DynamicRecord> recordIds = new HashMap<>();
                private List<DynamicRecord> recordList = new ArrayList<>();
                private boolean allInUse = true;

                @Override
                public void checkReference( NodeRecord record, DynamicRecord dynamicRecord,
                                            ConsistencyReport.NodeConsistencyReport report, RecordAccess records )
                {
                    recordIds.put( dynamicRecord.getId(), dynamicRecord );

                    if ( dynamicRecord.inUse() )
                    {
                        if ( allInUse )
                        {
                            recordList.add( dynamicRecord );
                        }
                    }
                    else
                    {
                        allInUse = false;
                        report.dynamicLabelRecordNotInUse( dynamicRecord );
                    }

                    long nextBlock = dynamicRecord.getNextBlock();
                    if ( Record.NO_NEXT_BLOCK.is( nextBlock ) )
                    {
                        if ( allInUse )
                        {
                            // only validate label ids if all dynamic records seen were in use
                            validateLabelIds( labelIds( recordList ), report, records );
                        }
                    }
                    else
                    {
                        if ( recordIds.containsKey( nextBlock ) )
                        {
                            report.dynamicRecordChainCycle( recordIds.get( nextBlock ) );
                        }
                        else
                        {
                            report.forReference( records.nodeLabels( nextBlock ), this );
                        }
                    }
                }

                private long[] labelIds( List<DynamicRecord> recordList )
                {
                    long[] idArray =
                        (long[]) getRightArray( readFullByteArrayFromHeavyRecords( recordList, PropertyType.ARRAY ) );
                    return copyOfRange( idArray, 1, idArray.length );
                }
            }
        }
    }
}
