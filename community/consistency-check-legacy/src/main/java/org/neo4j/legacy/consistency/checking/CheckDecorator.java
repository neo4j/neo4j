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

import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;

public interface CheckDecorator
{
    OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
            OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker );

    OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker );

    OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker );

    RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker );

    RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
            RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker );

    RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
            RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker );

    RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> decorateLabelTokenChecker(
            RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker );

    RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> decorateLabelMatchChecker(
            RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> checker );

    RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
            RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> checker );

    static CheckDecorator NONE = new Adapter();

    static class Adapter implements CheckDecorator
    {
        @Override
        public OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> decorateLabelMatchChecker(
                RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
                RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
        {
            return checker;
        }
    }

    static class ChainCheckDecorator implements CheckDecorator
    {
        private final CheckDecorator[] decorators;

        public ChainCheckDecorator( CheckDecorator...decorators )
        {
            this.decorators = decorators;
        }

        @Override
        public OwningRecordCheck<NeoStoreRecord,ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord,ConsistencyReport.NeoStoreConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateNeoStoreChecker( checker );
            }
            return checker;
        }

        @Override
        public OwningRecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord,ConsistencyReport.NodeConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateNodeChecker( checker );
            }
            return checker;
        }

        @Override
        public OwningRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord,ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateRelationshipChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord,ConsistencyReport.PropertyConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decoratePropertyChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord,ConsistencyReport.PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord,ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decoratePropertyKeyTokenChecker( checker );
            }
            return checker;

        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord,ConsistencyReport.RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord,ConsistencyReport.RelationshipTypeConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateRelationshipTypeTokenChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<LabelTokenRecord,ConsistencyReport.LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord,ConsistencyReport.LabelTokenConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateLabelTokenChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<NodeRecord,ConsistencyReport.LabelsMatchReport> decorateLabelMatchChecker(
                RecordCheck<NodeRecord,ConsistencyReport.LabelsMatchReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateLabelMatchChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
                RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> checker )
        {
            for ( CheckDecorator decorator: decorators)
            {
                checker = decorator.decorateRelationshipGroupChecker( checker );
            }
            return checker;
        }
    }
}
