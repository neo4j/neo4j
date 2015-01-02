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
package org.neo4j.consistency.checking.full;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.LabelScanDocument;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;

public class NullReporter implements ConsistencyReport.Reporter
{
    @Override
    public void forSchema( DynamicRecord schema, RecordCheck<DynamicRecord, ConsistencyReport.SchemaConsistencyReport
                > checker )
    {
    }

    @Override
    public void forSchemaChange( DynamicRecord oldSchema, DynamicRecord newSchema, RecordCheck<DynamicRecord,
            ConsistencyReport.SchemaConsistencyReport> checker )
    {
    }

    @Override
    public void forNode( NodeRecord node, RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
    }

    @Override
    public void forNodeChange( NodeRecord oldNode, NodeRecord newNode, RecordCheck<NodeRecord, ConsistencyReport
            .NodeConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationship( RelationshipRecord relationship, RecordCheck<RelationshipRecord, ConsistencyReport
            .RelationshipConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationshipChange( RelationshipRecord oldRelationship, RelationshipRecord newRelationship, RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
    }

    @Override
    public void forProperty( PropertyRecord property, RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
    }

    @Override
    public void forPropertyChange( PropertyRecord oldProperty, PropertyRecord newProperty,
                                   RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationshipTypeName( RelationshipTypeTokenRecord relationshipType, RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
    }

    @Override
    public void forRelationshipTypeNameChange( RelationshipTypeTokenRecord oldType, RelationshipTypeTokenRecord newType, RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
    }

    @Override
    public void forLabelName( LabelTokenRecord label, RecordCheck<LabelTokenRecord, ConsistencyReport
            .LabelTokenConsistencyReport> checker )
    {
    }

    @Override
    public void forLabelNameChange( LabelTokenRecord oldLabel, LabelTokenRecord newLabel,
                                    RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport>
                                            checker )
    {
    }

    @Override
    public void forPropertyKey( PropertyKeyTokenRecord key, RecordCheck<PropertyKeyTokenRecord,
            ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
    }

    @Override
    public void forPropertyKeyChange( PropertyKeyTokenRecord oldKey, PropertyKeyTokenRecord newKey, RecordCheck
            <PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicBlock( RecordType type, DynamicRecord record, RecordCheck<DynamicRecord, ConsistencyReport
            .DynamicConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord, RecordCheck
            <DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicLabelBlock( RecordType type, DynamicRecord record, RecordCheck<DynamicRecord, ConsistencyReport.DynamicLabelConsistencyReport> checker )
    {
    }

    @Override
    public void forDynamicLabelBlockChange( RecordType type, DynamicRecord oldRecord, DynamicRecord newRecord, RecordCheck<DynamicRecord, ConsistencyReport.DynamicLabelConsistencyReport> checker )
    {
    }

    @Override
    public void forNodeLabelScan( LabelScanDocument document, RecordCheck<LabelScanDocument, ConsistencyReport.LabelScanConsistencyReport> checker )
    {
    }

    @Override
    public void forIndexEntry( IndexEntry entry, RecordCheck<IndexEntry, ConsistencyReport.IndexConsistencyReport>
            checker )
    {
    }

    @Override
    public void forNodeLabelMatch( NodeRecord nodeRecord, RecordCheck<NodeRecord, ConsistencyReport.LabelsMatchReport> nodeLabelCheck )
    {
    }
}
