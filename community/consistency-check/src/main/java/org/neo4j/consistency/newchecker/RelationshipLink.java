/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.consistency.newchecker;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * Means of parameterizing selection of {@link RelationshipRecord} pointers.
 */
enum RelationshipLink
{
    SOURCE_PREV
    {
        @Override
        boolean endOfChain( RelationshipRecord relationship )
        {
            return relationship.isFirstInFirstChain();
        }

        @Override
        long node( RelationshipRecord relationship )
        {
            return relationship.getFirstNode();
        }

        @Override
        long link( RelationshipRecord relationship )
        {
            return relationship.getFirstPrevRel();
        }

        @Override
        void setOther( RelationshipRecord relationship, NodeLink nodeLink, long other )
        {
            nodeLink.setNextRel( relationship, other );
        }

        @Override
        long other( RelationshipRecord relationship, NodeLink nodeLink )
        {
            return nodeLink.getNextRel( relationship );
        }

        @Override
        void reportDoesNotReferenceBack( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).sourcePrevDoesNotReferenceBack( other );
        }

        @Override
        void reportNotUsedRelationshipReferencedInChain( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).notUsedRelationshipReferencedInChain( relationship );
        }

        @Override
        void reportOtherNode( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).sourcePrevReferencesOtherNodes( other );
        }
    },
    SOURCE_NEXT
    {
        @Override
        boolean endOfChain( RelationshipRecord relationship )
        {
            return NULL_REFERENCE.is( relationship.getFirstNextRel() );
        }

        @Override
        long node( RelationshipRecord relationship )
        {
            return relationship.getFirstNode();
        }

        @Override
        long link( RelationshipRecord relationship )
        {
            return relationship.getFirstNextRel();
        }

        @Override
        void setOther( RelationshipRecord relationship, NodeLink nodeLink, long other )
        {
            nodeLink.setPrevRel( relationship, other );
        }

        @Override
        long other( RelationshipRecord relationship, NodeLink nodeLink )
        {
            return nodeLink.getPrevRel( relationship );
        }

        @Override
        void reportDoesNotReferenceBack( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).sourceNextDoesNotReferenceBack( other );
        }

        @Override
        void reportNotUsedRelationshipReferencedInChain( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).notUsedRelationshipReferencedInChain( relationship );
        }

        @Override
        void reportOtherNode( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).sourceNextReferencesOtherNodes( other );
        }
    },
    TARGET_PREV
    {
        @Override
        boolean endOfChain( RelationshipRecord relationship )
        {
            return relationship.isFirstInSecondChain();
        }

        @Override
        long node( RelationshipRecord relationship )
        {
            return relationship.getSecondNode();
        }

        @Override
        long link( RelationshipRecord relationship )
        {
            return relationship.getSecondPrevRel();
        }

        @Override
        void setOther( RelationshipRecord relationship, NodeLink nodeLink, long other )
        {
            nodeLink.setNextRel( relationship, other );
        }

        @Override
        long other( RelationshipRecord relationship, NodeLink nodeLink )
        {
            return nodeLink.getNextRel( relationship );
        }

        @Override
        void reportDoesNotReferenceBack( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).targetPrevDoesNotReferenceBack( other );
        }

        @Override
        void reportNotUsedRelationshipReferencedInChain( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).notUsedRelationshipReferencedInChain( relationship );
        }

        @Override
        void reportOtherNode( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).targetPrevReferencesOtherNodes( other );
        }
    },
    TARGET_NEXT
    {
        @Override
        boolean endOfChain( RelationshipRecord relationship )
        {
            return NULL_REFERENCE.is( relationship.getSecondNextRel() );
        }

        @Override
        long node( RelationshipRecord relationship )
        {
            return relationship.getSecondNode();
        }

        @Override
        long link( RelationshipRecord relationship )
        {
            return relationship.getSecondNextRel();
        }

        @Override
        void setOther( RelationshipRecord relationship, NodeLink nodeLink, long other )
        {
            nodeLink.setPrevRel( relationship, other );
        }

        @Override
        long other( RelationshipRecord relationship, NodeLink nodeLink )
        {
            return nodeLink.getPrevRel( relationship );
        }

        @Override
        void reportDoesNotReferenceBack( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).targetNextDoesNotReferenceBack( other );
        }

        @Override
        void reportNotUsedRelationshipReferencedInChain( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).notUsedRelationshipReferencedInChain( relationship );
        }

        @Override
        void reportOtherNode( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other )
        {
            reporter.forRelationship( relationship ).targetNextReferencesOtherNodes( other );
        }
    };

    abstract boolean endOfChain( RelationshipRecord relationship );

    abstract long node( RelationshipRecord relationship );

    abstract long link( RelationshipRecord relationship );

    abstract void setOther( RelationshipRecord relationship, NodeLink nodeLink, long other );

    abstract long other( RelationshipRecord relationship, NodeLink nodeLink );

    abstract void reportDoesNotReferenceBack( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other );

    abstract void reportNotUsedRelationshipReferencedInChain( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other );

    abstract void reportOtherNode( ConsistencyReport.Reporter reporter, RelationshipRecord relationship, RelationshipRecord other );
}
