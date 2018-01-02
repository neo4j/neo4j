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

import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;
import org.neo4j.legacy.consistency.store.RecordReference;

import static java.util.Arrays.sort;

class NodeRecordCheck extends PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    static NodeRecordCheck forSparseNodes()
    {
        return new NodeRecordCheck( RelationshipField.NEXT_REL, LabelsField.LABELS );
    }

    static NodeRecordCheck forDenseNodes()
    {
        return new NodeRecordCheck( RelationshipGroupField.NEXT_GROUP, LabelsField.LABELS );
    }

    @SafeVarargs
    private NodeRecordCheck( RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>... fields )
    {
        super( fields );
    }

    NodeRecordCheck()
    {
        super( RelationshipField.NEXT_REL, LabelsField.LABELS );
    }

    private enum RelationshipGroupField implements RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>,
            ComparativeRecordChecker<NodeRecord, RelationshipGroupRecord, ConsistencyReport.NodeConsistencyReport>
    {
        NEXT_GROUP
        {
            @Override
            public void checkConsistency( NodeRecord node, CheckerEngine<NodeRecord, NodeConsistencyReport> engine,
                    RecordAccess records )
            {
                if ( !Record.NO_NEXT_RELATIONSHIP.is( node.getNextRel() ) )
                {
                    engine.comparativeCheck( records.relationshipGroup( node.getNextRel() ), this );
                }
            }

            @Override
            public long valueFrom( NodeRecord node )
            {
                return node.getNextRel();
            }

            @Override
            public void checkChange( NodeRecord oldRecord, NodeRecord newRecord,
                    CheckerEngine<NodeRecord, NodeConsistencyReport> engine, DiffRecordAccess records )
            {
                if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
                {
                    if ( oldRecord.isDense() == newRecord.isDense() )
                    {
                        // TODO we can do this check if/when relationship group records gets prev pointers,
                        // until then the previous group is actually unchanged when adding a new in front.
//                        if ( !Record.NO_NEXT_RELATIONSHIP.is( valueFrom( oldRecord ) )
//                                && records.changedRelationshipGroup( valueFrom( oldRecord ) ) == null )
//                        {
//                            engine.report().relationshipGroupNotUpdated();
//                        }
                    }
                    else
                    {
                        // TODO the node just got upgraded to dense in this transaction
                    }
                }
            }

            @Override
            public void checkReference( NodeRecord record, RelationshipGroupRecord group,
                    CheckerEngine<NodeRecord, NodeConsistencyReport> engine, RecordAccess records )
            {
                if ( !group.inUse() )
                {
                    engine.report().relationshipGroupNotInUse( group );
                }
                else
                {
                    if ( group.getOwningNode() != record.getId() )
                    {
                        engine.report().relationshipGroupHasOtherOwner( group );
                    }
                }
            }
        }
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
                            if ( !field.isFirst( relationship ) )
                            {
                                field.notFirstInChain( engine.report(), relationship );
                            }
                            // TODO we should check that the number of relationships in the chain match
                            // the value in the "prev" field.
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
                // This first loop, before sorting happens, verifies that labels are ordered like they are supposed to
                boolean outOfOrder = false;
                for ( int i = 1; i < labelIds.length; i++ )
                {
                    if ( labelIds[i -1] > labelIds[i])
                    {
                        engine.report().labelsOutOfOrder( labelIds[i-1], labelIds[i] );
                        outOfOrder = true;
                    }
                }
                if ( outOfOrder )
                {
                    sort( labelIds );
                }
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
