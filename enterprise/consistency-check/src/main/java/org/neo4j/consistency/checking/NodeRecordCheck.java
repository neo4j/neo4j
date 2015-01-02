/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.labels.DynamicNodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField;

import static java.util.Arrays.sort;

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
            public void checkConsistency( NodeRecord node,
                                          CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                          RecordAccess records )
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.is( node.getNextRel() ) )
                {
                    engine.comparativeCheck( records.relationship( node.getNextRel() ), this );
                }
            }

            @Override
            public void checkReference( NodeRecord node, RelationshipRecord relationship,
                                        CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                        RecordAccess records )
            {
                if ( !relationship.inUse() )
                {
                    engine.report().relationshipNotInUse( relationship );
                }
                else
                {
                    NodeField selectedField = NodeField.select( relationship, node );
                    if ( selectedField == null )
                    {
                        engine.report().relationshipForOtherNode( relationship );
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
                                field.notFirstInChain( engine.report(), relationship );
                            }
                        }
                    }
                }
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                     DiffRecordAccess records )
            {
                if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
                {
                    if ( !Record.NO_NEXT_RELATIONSHIP.is( valueFrom( oldRecord ) )
                         && records.changedRelationship( valueFrom( oldRecord ) ) == null )
                    {
                        engine.report().relationshipNotUpdated();
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
            public void checkConsistency( NodeRecord node,
                                          CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                          RecordAccess records )
            {
                NodeLabels nodeLabels = NodeLabelsField.parseLabelsField( node );
                if ( nodeLabels instanceof DynamicNodeLabels )
                {
                    DynamicNodeLabels dynamicNodeLabels = (DynamicNodeLabels) nodeLabels;
                    long firstRecordId = dynamicNodeLabels.getFirstDynamicRecordId();
                    RecordReference<DynamicRecord> firstRecordReference = records.nodeLabels( firstRecordId );
                    engine.comparativeCheck( firstRecordReference,
                            new LabelChainWalker<NodeRecord, ConsistencyReport.NodeConsistencyReport>(
                                    new NodeLabelsComparativeRecordChecker() ) );
                }
                else
                {
                    validateLabelIds( nodeLabels.get( null ), engine, records );
                }
            }

            private void validateLabelIds( long[] labelIds,
                                           CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                           RecordAccess records )
            {
                for ( long labelId : labelIds )
                {
                    engine.comparativeCheck( records.label( (int) labelId ), this );
                }
                sort( labelIds );
                for ( int i = 1; i < labelIds.length; i++ )
                {
                    if ( labelIds[i - 1] == labelIds[i] )
                    {
                        engine.report().labelDuplicate( labelIds[i] );
                    }
                }
            }

            @Override
            public void checkReference( NodeRecord node, LabelTokenRecord labelTokenRecord,
                                        CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
                                        RecordAccess records )
            {
                if ( !labelTokenRecord.inUse() )
                {
                    engine.report().labelNotInUse( labelTokenRecord );
                }
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                                     CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine,
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
                    LabelChainWalker.Validator<NodeRecord, ConsistencyReport.NodeConsistencyReport>
            {
                @Override
                public void onRecordNotInUse( DynamicRecord dynamicRecord, CheckerEngine<NodeRecord,
                        ConsistencyReport.NodeConsistencyReport> engine )
                {
                    engine.report().dynamicLabelRecordNotInUse( dynamicRecord );
                }

                @Override
                public void onRecordChainCycle( DynamicRecord record, CheckerEngine<NodeRecord, ConsistencyReport
                        .NodeConsistencyReport> engine )
                {
                    engine.report().dynamicRecordChainCycle( record );
                }

                @Override
                public void onWellFormedChain( long[] labelIds, CheckerEngine<NodeRecord, ConsistencyReport.NodeConsistencyReport> engine, RecordAccess records )
                {
                    validateLabelIds( labelIds, engine, records );
                }
            }
        }
    }
}
