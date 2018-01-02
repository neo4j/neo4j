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

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.store.DiffRecordAccess;
import org.neo4j.legacy.consistency.store.RecordAccess;

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
        SOURCE_PREV( NodeField.SOURCE )
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

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }
        },
        SOURCE_NEXT( NodeField.SOURCE )
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

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }
        },
        TARGET_PREV( NodeField.TARGET )
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

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.isFirst( record );
            }
        },
        TARGET_NEXT( NodeField.TARGET )
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

            @Override
            boolean endOfChain( RelationshipRecord record )
            {
                return NODE.next( record ) == Record.NO_NEXT_RELATIONSHIP.intValue();
            }
        };

        protected final NodeField NODE;

        private RelationshipField( NodeField node )
        {
            this.NODE = node;
        }

        @Override
        public void checkConsistency( RelationshipRecord relationship,
                                      CheckerEngine<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> engine,
                                      RecordAccess records )
        {
            if ( !endOfChain( relationship ) )
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
            {   // if we're deleting or creating this relationship record
                if ( !endOfChain( oldRecord )
                     && records.changedRelationship( valueFrom( oldRecord ) ) == null )
                {   // and we didn't update an expected pointer --> report
                    notUpdated( engine.report() );
                }
            }
        }

        abstract boolean endOfChain( RelationshipRecord record );

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
