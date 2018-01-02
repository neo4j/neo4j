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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.store.RecordAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static java.util.Arrays.asList;

public class RelationshipGroupRecordCheck implements
        RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>
{
    private final static List<RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>> fields;
    static
    {
        List<RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>> list = new ArrayList<>();
        list.add( RelationshipTypeField.RELATIONSHIP_TYPE );
        list.add( GroupField.NEXT );
        list.add( NodeField.OWNER );
        list.addAll( asList( RelationshipField.values() ) );
        fields = Collections.unmodifiableList( list );
    }

    private enum NodeField implements
            RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>,
            ComparativeRecordChecker<RelationshipGroupRecord, NodeRecord, ConsistencyReport.RelationshipGroupConsistencyReport>
    {
        OWNER;

        @Override
        public void checkReference( RelationshipGroupRecord record, NodeRecord referred,
                CheckerEngine<RelationshipGroupRecord, RelationshipGroupConsistencyReport> engine, RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().ownerNotInUse();
            }
        }

        @Override
        public void checkConsistency( RelationshipGroupRecord record,
                CheckerEngine<RelationshipGroupRecord, RelationshipGroupConsistencyReport> engine, RecordAccess records )
        {
            if ( record.getOwningNode() < 0 )
            {
                engine.report().illegalOwner();
            }
            else
            {
                engine.comparativeCheck( records.node( record.getOwningNode() ), this );
            }
        }

        @Override
        public long valueFrom( RelationshipGroupRecord record )
        {
            return record.getOwningNode();
        }
    }

    private enum RelationshipTypeField implements
            RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>,
            ComparativeRecordChecker<RelationshipGroupRecord, RelationshipTypeTokenRecord, ConsistencyReport.RelationshipGroupConsistencyReport>
    {
        RELATIONSHIP_TYPE;

        @Override
        public void checkConsistency( RelationshipGroupRecord record,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            if ( record.getType() < 0 )
            {
                engine.report().illegalRelationshipType();
            }
            else
            {
                engine.comparativeCheck( records.relationshipType( record.getType() ), this );
            }
        }

        @Override
        public long valueFrom( RelationshipGroupRecord record )
        {
            return record.getType();
        }

        @Override
        public void checkReference( RelationshipGroupRecord record, RelationshipTypeTokenRecord referred,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().relationshipTypeNotInUse( referred );
            }
        }
    }

    private enum GroupField implements
            RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>,
            ComparativeRecordChecker<RelationshipGroupRecord, RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>
    {
        NEXT;

        @Override
        public void checkConsistency( RelationshipGroupRecord record,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            if ( record.getNext() != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                engine.comparativeCheck( records.relationshipGroup( record.getNext() ), this );
            }
        }

        @Override
        public long valueFrom( RelationshipGroupRecord record )
        {
            return record.getNext();
        }

        @Override
        public void checkReference( RelationshipGroupRecord record, RelationshipGroupRecord referred,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().nextGroupNotInUse();
            }
            else
            {
                if ( record.getType() >= referred.getType() )
                {
                    engine.report().invalidTypeSortOrder();
                }
                if ( record.getOwningNode() != referred.getOwningNode() )
                {
                    engine.report().nextHasOtherOwner( referred );
                }
            }
        }
    }

    private enum RelationshipField implements
            RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport>,
            ComparativeRecordChecker<RelationshipGroupRecord, RelationshipRecord, ConsistencyReport.RelationshipGroupConsistencyReport>
    {
        OUT
        {
            @Override
            public long valueFrom( RelationshipGroupRecord record )
            {
                return record.getFirstOut();
            }

            @Override
            protected void relationshipNotInUse( RelationshipGroupConsistencyReport report )
            {
                report.firstOutgoingRelationshipNotInUse();
            }

            @Override
            protected void relationshipNotFirstInChain( RelationshipGroupConsistencyReport report )
            {
                report.firstOutgoingRelationshipNotFirstInChain();
            }

            @Override
            protected boolean isFirstInChain( RelationshipRecord relationship )
            {
                return relationship.isFirstInFirstChain();
            }

            @Override
            protected void relationshipOfOtherType( RelationshipGroupConsistencyReport report )
            {
                report.firstOutgoingRelationshipOfOfOtherType();
            }
        },
        IN
        {
            @Override
            public long valueFrom( RelationshipGroupRecord record )
            {
                return record.getFirstIn();
            }

            @Override
            protected void relationshipNotInUse( RelationshipGroupConsistencyReport report )
            {
                report.firstIncomingRelationshipNotInUse();
            }

            @Override
            protected void relationshipNotFirstInChain( RelationshipGroupConsistencyReport report )
            {
                report.firstIncomingRelationshipNotFirstInChain();
            }

            @Override
            protected boolean isFirstInChain( RelationshipRecord relationship )
            {
                return relationship.isFirstInSecondChain();
            }

            @Override
            protected void relationshipOfOtherType( RelationshipGroupConsistencyReport report )
            {
                report.firstIncomingRelationshipOfOfOtherType();
            }
        },
        LOOP
        {
            @Override
            public long valueFrom( RelationshipGroupRecord record )
            {
                return record.getFirstLoop();
            }

            @Override
            protected void relationshipNotInUse( RelationshipGroupConsistencyReport report )
            {
                report.firstLoopRelationshipNotInUse();
            }

            @Override
            protected void relationshipNotFirstInChain( RelationshipGroupConsistencyReport report )
            {
                report.firstLoopRelationshipNotFirstInChain();
            }

            @Override
            protected boolean isFirstInChain( RelationshipRecord relationship )
            {
                return relationship.isFirstInSecondChain() && relationship.isFirstInSecondChain();
            }

            @Override
            protected void relationshipOfOtherType( RelationshipGroupConsistencyReport report )
            {
                report.firstLoopRelationshipOfOfOtherType();
            }
        };

        @Override
        public void checkConsistency( RelationshipGroupRecord record,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            long relId = valueFrom( record );
            if ( relId != Record.NO_NEXT_RELATIONSHIP.intValue() )
            {
                engine.comparativeCheck( records.relationship( relId ), this );
            }
        }

        @Override
        public void checkReference( RelationshipGroupRecord record, RelationshipRecord referred,
                CheckerEngine<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> engine,
                RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                relationshipNotInUse( engine.report() );
            }
            else
            {
                if ( !isFirstInChain( referred ) )
                {
                    relationshipNotFirstInChain( engine.report() );
                }
                if ( referred.getType() != record.getType() )
                {
                    relationshipOfOtherType( engine.report() );
                }
            }
        }

        protected abstract void relationshipOfOtherType( RelationshipGroupConsistencyReport report );

        protected abstract void relationshipNotFirstInChain( RelationshipGroupConsistencyReport report );

        protected abstract boolean isFirstInChain( RelationshipRecord referred );

        protected abstract void relationshipNotInUse( RelationshipGroupConsistencyReport report );
    }

    @Override
    public void check( RelationshipGroupRecord record,
            CheckerEngine<RelationshipGroupRecord, RelationshipGroupConsistencyReport> engine, RecordAccess records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( RecordField<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> field : fields )
        {
            field.checkConsistency( record, engine, records );
        }
    }
}
