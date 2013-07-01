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
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

enum NodeField implements
        RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
        ComparativeRecordChecker<RelationshipRecord, NodeRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    SOURCE
    {
        @Override
        public long valueFrom( RelationshipRecord relationship )
        {
            return relationship.getFirstNode();
        }

        @Override
        public long prev( RelationshipRecord relationship )
        {
            return relationship.getFirstPrevRel();
        }

        @Override
        public long next( RelationshipRecord relationship )
        {
            return relationship.getFirstNextRel();
        }

        @Override
        void illegalNode( ConsistencyReport.RelationshipConsistencyReport report )
        {
            report.illegalSourceNode();
        }

        @Override
        void nodeNotInUse( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.sourceNodeNotInUse( node );
        }

        @Override
        void noBackReference( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.sourceNodeDoesNotReferenceBack( node );
        }

        @Override
        void noChain( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.sourceNodeHasNoRelationships( node );
        }

        @Override
        void notFirstInChain( ConsistencyReport.NodeConsistencyReport report, RelationshipRecord relationship )
        {
            report.relationshipNotFirstInSourceChain( relationship );
        }

        @Override
        void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
        {
            report.sourceNodeNotUpdated();
        }
    },
    TARGET
    {
        @Override
        public long valueFrom( RelationshipRecord relationship )
        {
            return relationship.getSecondNode();
        }

        @Override
        public long prev( RelationshipRecord relationship )
        {
            return relationship.getSecondPrevRel();
        }

        @Override
        public long next( RelationshipRecord relationship )
        {
            return relationship.getSecondNextRel();
        }

        @Override
        void illegalNode( ConsistencyReport.RelationshipConsistencyReport report )
        {
            report.illegalTargetNode();
        }

        @Override
        void nodeNotInUse( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.targetNodeNotInUse( node );
        }

        @Override
        void noBackReference( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.targetNodeDoesNotReferenceBack( node );
        }

        @Override
        void noChain( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node )
        {
            report.targetNodeHasNoRelationships( node );
        }

        @Override
        void notFirstInChain( ConsistencyReport.NodeConsistencyReport report, RelationshipRecord relationship )
        {
            report.relationshipNotFirstInTargetChain( relationship );
        }

        @Override
        void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
        {
            report.targetNodeNotUpdated();
        }
    };

    @Override
    public abstract long valueFrom( RelationshipRecord relationship );

    public static NodeField select( RelationshipRecord relationship, NodeRecord node )
    {
        return select( relationship, node.getId() );
    }

    public static NodeField select( RelationshipRecord relationship, long nodeId )
    {
        if ( relationship.getFirstNode() == nodeId )
        {
            return SOURCE;
        }
        else if ( relationship.getSecondNode() == nodeId )
        {
            return TARGET;
        }
        else
        {
            return null;
        }
    }

    public abstract long prev( RelationshipRecord relationship );

    public abstract long next( RelationshipRecord relationship );

    @Override
    public void checkConsistency( RelationshipRecord relationship,
                                  ConsistencyReport.RelationshipConsistencyReport report,
                                  RecordAccess records )
    {
        if ( valueFrom( relationship ) < 0 )
        {
            illegalNode( report );
        }
        else
        {
            report.forReference( records.node( valueFrom( relationship ) ), this );
        }
    }

    @Override
    public void checkReference( RelationshipRecord relationship, NodeRecord node,
                                ConsistencyReport.RelationshipConsistencyReport report, RecordAccess records )
    {
        if ( !node.inUse() )
        {
            nodeNotInUse( report, node );
        }
        else
        {
            if ( Record.NO_PREV_RELATIONSHIP.is( prev( relationship ) ) )
            {
                if ( node.getNextRel() != relationship.getId() )
                {
                    noBackReference( report, node );
                }
            }
            else
            {
                if ( Record.NO_NEXT_RELATIONSHIP.is( node.getNextRel() ) )
                {
                    noChain( report, node );
                }
            }
        }
    }

    @Override
    public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                             ConsistencyReport.RelationshipConsistencyReport report, DiffRecordAccess records )
    {
        if ( Record.NO_PREV_RELATIONSHIP.is( prev( oldRecord ) ) )
        {
            if ( !newRecord.inUse()
                 || valueFrom( oldRecord ) != valueFrom( newRecord )
                 || prev( oldRecord ) != prev( newRecord ) )
            {
                if ( records.changedNode( valueFrom( oldRecord ) ) == null )
                {
                    notUpdated(report);
                }
            }
        }
    }

    abstract void notUpdated( ConsistencyReport.RelationshipConsistencyReport report );

    abstract void illegalNode( ConsistencyReport.RelationshipConsistencyReport report );

    abstract void nodeNotInUse( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void noChain( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void notFirstInChain( ConsistencyReport.NodeConsistencyReport report, RelationshipRecord relationship );
}
