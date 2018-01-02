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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink;
import org.neo4j.consistency.checking.full.MultiPassStore;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.DirectRecordReference;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.NEXT;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_PREV_OR_NEXT;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_RELATIONSHIP_ID;
import static org.neo4j.consistency.checking.cache.CacheSlots.RelationshipLink.SLOT_SOURCE_OR_TARGET;

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
        public boolean isFirst( RelationshipRecord relationship )
        {
            return relationship.isFirstInFirstChain();
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
        public boolean isFirst( RelationshipRecord relationship )
        {
            return relationship.isFirstInSecondChain();
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

    public abstract boolean isFirst( RelationshipRecord relationship );

    public boolean hasRelationship( NodeRecord node )
    {
        return node.getNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue();
    }

    @Override
    public void checkConsistency( RelationshipRecord relationship,
                                  CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                  RecordAccess records )
    {
        if ( valueFrom( relationship ) < 0 )
        {
            illegalNode( engine.report() );
        }
        else
        {
            // build the node record from cached values with only valid fields as id, inUse, and nextRel.
            NodeRecord node = new NodeRecord(valueFrom( relationship ));
            CacheAccess.Client client = records.cacheAccess().client();
            node.setInUse( client.getFromCache( node.getId(), SLOT_SOURCE_OR_TARGET ) != RelationshipLink.SOURCE );
            node.setNextRel( client.getFromCache( node.getId(), SLOT_RELATIONSHIP_ID ) );

            // We use "created" flag here. Consistency checking code revolves around records and so
            // even in scenarios where records are built from other sources, f.ex half-and-purpose-built from cache,
            // this flag is used to signal that the real record needs to be read in order to be used as a general
            // purpose record.
            node.setCreated();
            if ( records.shouldCheck( node.getId(), MultiPassStore.NODES ) )
            {
                engine.comparativeCheck( new DirectRecordReference<>( node, records ), this );
            }
        }
    }

    @Override
    public void checkReference( RelationshipRecord relationship, NodeRecord node,
                                CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                RecordAccess records )
    {
        if ( !node.inUse() )
        {
            nodeNotInUse( engine.report(), node );
        }
        else
        {
            if ( isFirst( relationship ) )
            {
                CacheAccess.Client cacheAccess = records.cacheAccess().client();
                if ( node.getNextRel() != relationship.getId() )
                {
                    node = ((DirectRecordReference<NodeRecord>)records.node( node.getId())).record();

                    if ( node.isDense() )
                    {
                        // TODO verify that the appropriate group refers back to the relationship
                    }
                    else
                    {
                        noBackReference( engine.report(), node );
                    }
                }
                else
                {
                    if ( relationship.getFirstNode() != relationship.getSecondNode() )
                    {
                        cacheAccess.putToCacheSingle( node.getId(), SLOT_PREV_OR_NEXT, NEXT );
                    }
                }
            }
            else
            {
                if ( !hasRelationship( node ) )
                {
                    noChain( engine.report(), node );
                }
            }
        }
    }

    abstract void illegalNode( ConsistencyReport.RelationshipConsistencyReport report );

    abstract void nodeNotInUse( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void noChain( ConsistencyReport.RelationshipConsistencyReport report, NodeRecord node );

    abstract void notFirstInChain( ConsistencyReport.NodeConsistencyReport report, RelationshipRecord relationship );
}
