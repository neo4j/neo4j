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
package org.neo4j.consistency.checking;

import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NeoStoreConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public interface CheckDecorator
{
    /**
     * Called before each pass over the store(s) to check.
     */
    void prepare();

    OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> decorateNeoStoreChecker(
            OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> checker );

    OwningRecordCheck<NodeRecord, NodeConsistencyReport> decorateNodeChecker(
            OwningRecordCheck<NodeRecord, NodeConsistencyReport> checker );

    OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> decorateRelationshipChecker(
            OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker );

    RecordCheck<PropertyRecord, PropertyConsistencyReport> decoratePropertyChecker(
            RecordCheck<PropertyRecord, PropertyConsistencyReport> checker );

    RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
            RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker );

    RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
            RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker );

    RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> decorateLabelTokenChecker(
            RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker );

    RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker );

    CheckDecorator NONE = new Adapter();

    class Adapter implements CheckDecorator
    {
        @Override
        public void prepare()
        {
        }

        @Override
        public OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord, NeoStoreConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public OwningRecordCheck<NodeRecord, NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord, NodeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyRecord, PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord, PropertyConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker )
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

    class ChainCheckDecorator implements CheckDecorator
    {
        private final CheckDecorator[] decorators;

        public ChainCheckDecorator( CheckDecorator...decorators )
        {
            this.decorators = decorators;
        }

        @Override
        public void prepare()
        {
            for ( CheckDecorator decorator : decorators )
            {
                decorator.prepare();
            }
        }

        @Override
        public OwningRecordCheck<NeoStoreRecord,NeoStoreConsistencyReport> decorateNeoStoreChecker(
                OwningRecordCheck<NeoStoreRecord,NeoStoreConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateNeoStoreChecker( checker );
            }
            return checker;
        }

        @Override
        public OwningRecordCheck<NodeRecord,NodeConsistencyReport> decorateNodeChecker(
                OwningRecordCheck<NodeRecord,NodeConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateNodeChecker( checker );
            }
            return checker;
        }

        @Override
        public OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> decorateRelationshipChecker(
                OwningRecordCheck<RelationshipRecord,RelationshipConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateRelationshipChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<PropertyRecord,PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord,PropertyConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decoratePropertyChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<PropertyKeyTokenRecord,PropertyKeyTokenConsistencyReport> decoratePropertyKeyTokenChecker(
                RecordCheck<PropertyKeyTokenRecord,PropertyKeyTokenConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decoratePropertyKeyTokenChecker( checker );
            }
            return checker;

        }

        @Override
        public RecordCheck<RelationshipTypeTokenRecord,RelationshipTypeConsistencyReport> decorateRelationshipTypeTokenChecker(
                RecordCheck<RelationshipTypeTokenRecord,RelationshipTypeConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateRelationshipTypeTokenChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<LabelTokenRecord,LabelTokenConsistencyReport> decorateLabelTokenChecker(
                RecordCheck<LabelTokenRecord,LabelTokenConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateLabelTokenChecker( checker );
            }
            return checker;
        }

        @Override
        public RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> decorateRelationshipGroupChecker(
                RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> checker )
        {
            for ( CheckDecorator decorator : decorators )
            {
                checker = decorator.decorateRelationshipGroupChecker( checker );
            }
            return checker;
        }
    }
}
