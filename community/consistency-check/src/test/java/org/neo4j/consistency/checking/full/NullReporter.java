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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.IndexConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelScanConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

public class NullReporter implements ConsistencyReport.Reporter
{
    @Override
    public void forSchema( DynamicRecord schema, RecordCheck<DynamicRecord, ConsistencyReport.SchemaConsistencyReport
                > checker )
    {
    }

    @Override
    public void forNode( NodeRecord node, RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationship( RelationshipRecord relationship, RecordCheck<RelationshipRecord, ConsistencyReport
            .RelationshipConsistencyReport> checker )
    {
    }

    @Override
    public void forProperty( PropertyRecord property, RecordCheck<PropertyRecord, PropertyConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationshipTypeName( RelationshipTypeTokenRecord relationshipType,
            RecordCheck<RelationshipTypeTokenRecord,RelationshipTypeConsistencyReport> checker )
    {
    }

    @Override
    public void forLabelName( LabelTokenRecord label, RecordCheck<LabelTokenRecord, ConsistencyReport
            .LabelTokenConsistencyReport> checker )
    {
    }

    @Override
    public void forPropertyKey( PropertyKeyTokenRecord key, RecordCheck<PropertyKeyTokenRecord,
            ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicBlock( RecordType type, DynamicRecord record,
            RecordCheck<DynamicRecord,DynamicConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicLabelBlock( RecordType type, DynamicRecord record,
            RecordCheck<DynamicRecord,DynamicLabelConsistencyReport> checker )
    {
    }

    @Override
    public void forNodeLabelScan( LabelScanDocument document,
            RecordCheck<LabelScanDocument,LabelScanConsistencyReport> checker )
    {
    }

    @Override
    public void forIndexEntry( IndexEntry entry, RecordCheck<IndexEntry, IndexConsistencyReport>
            checker )
    {
    }

    @Override
    public void forRelationshipGroup( RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
    {
    }

    @Override
    public void forCounts( CountsEntry countsEntry,
                           RecordCheck<CountsEntry,ConsistencyReport.CountsConsistencyReport> checker )
    {
    }
}
