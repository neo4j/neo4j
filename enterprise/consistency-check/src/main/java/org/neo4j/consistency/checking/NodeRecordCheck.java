/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

class NodeRecordCheck extends PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport>
{
    NodeRecordCheck()
    {
        super( NodeField.RELATIONSHIP );
    }

    private enum NodeField implements RecordField<NodeRecord, ConsistencyReport.NodeConsistencyReport>,
                                      ComparativeRecordChecker<NodeRecord, RelationshipRecord, ConsistencyReport.NodeConsistencyReport>
    {
        RELATIONSHIP;

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
                RelationshipNodeField selectedField = RelationshipNodeField.select( relationship, node );
                if ( selectedField == null )
                {
                    report.relationshipForOtherNode( relationship );
                }
                else
                {
                    RelationshipNodeField[] fields;
                    if ( relationship.getFirstNode() == relationship.getSecondNode() )
                    { // this relationship is a loop, report both inconsistencies
                        fields = RelationshipNodeField.values();
                    }
                    else
                    {
                        fields = new RelationshipNodeField[]{selectedField};
                    }
                    for ( RelationshipNodeField field : fields )
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
