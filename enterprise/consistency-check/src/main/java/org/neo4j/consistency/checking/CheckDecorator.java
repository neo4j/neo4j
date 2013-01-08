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
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public interface CheckDecorator
{
    RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
            PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker );

    RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker );

    RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker );

    RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker );

    RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> decoratePropertyKeyChecker(
            RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker );

    RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> decorateLabelChecker(
            RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker );

    static CheckDecorator NONE = new CheckDecorator()
    {
        @Override
        public RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
                PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
                PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
                PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
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
        public RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> decoratePropertyKeyChecker(
                RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> decorateLabelChecker(
                RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            return checker;
        }
    };
}
