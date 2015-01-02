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
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

class RelationshipRecordCheck
        extends PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
{
    RelationshipRecordCheck()
    {
        super( RelationshipTypeField.RELATIONSHIP_TYPE,
               NodeField.SOURCE, RelationshipField.SOURCE_PREV, RelationshipField.SOURCE_NEXT,
               NodeField.TARGET, RelationshipField.TARGET_PREV, RelationshipField.TARGET_NEXT );
    }

    private enum RelationshipTypeField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipTypeTokenRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        RELATIONSHIP_TYPE;

        @Override
        public void checkConsistency( RelationshipRecord record,
                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
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
        public long valueFrom( RelationshipRecord record )
        {
            return record.getType();
        }

        @Override
        public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                 CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            // nothing to check
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipTypeTokenRecord referred,
                                    CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
            if ( !referred.inUse() )
            {
                engine.report().relationshipTypeNotInUse( referred );
            }
        }
    }

    private enum RelationshipField implements
            RecordField<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>,
            ComparativeRecordChecker<RelationshipRecord, RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport>
    {
        SOURCE_PREV( NodeField.SOURCE, Record.NO_PREV_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourcePrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.sourcePrevDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.sourcePrevNotUpdated();
            }
        },
        SOURCE_NEXT( NodeField.SOURCE, Record.NO_NEXT_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getFirstNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.sourceNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.sourceNextDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.sourceNextNotUpdated();
            }
        },
        TARGET_PREV( NodeField.TARGET, Record.NO_PREV_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondPrevRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.next( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetPrevReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.targetPrevDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.targetPrevNotUpdated();
            }
        },
        TARGET_NEXT( NodeField.TARGET, Record.NO_NEXT_RELATIONSHIP )
        {
            @Override
            public long valueFrom( RelationshipRecord relationship )
            {
                return relationship.getSecondNextRel();
            }

            @Override
            long other( NodeField field, RelationshipRecord relationship )
            {
                return field.prev( relationship );
            }

            @Override
            void otherNode( ConsistencyReport.RelationshipConsistencyReport report, RelationshipRecord relationship )
            {
                report.targetNextReferencesOtherNodes( relationship );
            }

            @Override
            void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                  RelationshipRecord relationship )
            {
                report.targetNextDoesNotReferenceBack( relationship );
            }

            @Override
            void notUpdated( ConsistencyReport.RelationshipConsistencyReport report )
            {
                report.targetNextNotUpdated();
            }
        };
        private final NodeField NODE;
        private final Record NONE;

        private RelationshipField( NodeField node, Record none )
        {
            this.NODE = node;
            this.NONE = none;
        }

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
            if ( !NONE.is( valueFrom( relationship ) ) )
            {
                engine.comparativeCheck( records.relationship( valueFrom( relationship ) ), this );
            }
        }

        @Override
        public void checkReference( RelationshipRecord record, RelationshipRecord referred,
                                    CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                    RecordAccess records )
        {
            NodeField field = NodeField.select( referred, node( record ) );
            if ( field == null )
            {
                otherNode( engine.report(), referred );
            }
            else
            {
                if ( other( field, referred ) != record.getId() )
                {
                    noBackReference( engine.report(), referred );
                }
            }
        }

        @Override
        public void checkChange( RelationshipRecord oldRecord, RelationshipRecord newRecord,
                                 CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                 DiffRecordAccess records )
        {
            if ( !newRecord.inUse() || valueFrom( oldRecord ) != valueFrom( newRecord ) )
            {
                if ( !NONE.is( valueFrom( oldRecord ) )
                     && records.changedRelationship( valueFrom( oldRecord ) ) == null )
                {
                    notUpdated( engine.report() );
                }
            }
        }

        abstract void notUpdated( ConsistencyReport.RelationshipConsistencyReport report );

        abstract long other( NodeField field, RelationshipRecord relationship );

        abstract void otherNode( ConsistencyReport.RelationshipConsistencyReport report,
                                 RelationshipRecord relationship );

        abstract void noBackReference( ConsistencyReport.RelationshipConsistencyReport report,
                                       RelationshipRecord relationship );

        private long node( RelationshipRecord relationship )
        {
            return NODE.valueFrom( relationship );
        }
    }
}
