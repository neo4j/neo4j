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
package org.neo4j.legacy.consistency.checking.full;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.legacy.consistency.RecordType;
import org.neo4j.legacy.consistency.checking.AbstractStoreProcessor;
import org.neo4j.legacy.consistency.checking.CheckDecorator;
import org.neo4j.legacy.consistency.checking.RecordCheck;
import org.neo4j.legacy.consistency.checking.SchemaRecordCheck;
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;

/**
 * Full check works by spawning StoreProcessorTasks that call StoreProcessor. StoreProcessor.applyFiltered()
 * then scans the store and in turn calls down to store.accept which then knows how to check the given record.
 */
class StoreProcessor extends AbstractStoreProcessor
{
    private final ConsistencyReport.Reporter report;
    private SchemaRecordCheck schemaRecordCheck;

    public StoreProcessor( CheckDecorator decorator, ConsistencyReport.Reporter report )
    {
        super( decorator );
        this.report = report;
        this.schemaRecordCheck = null;
    }

    @SuppressWarnings("UnusedParameters")
    protected void checkSchema( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord schema, RecordCheck
            <DynamicRecord, ConsistencyReport.SchemaConsistencyReport> checker )
    {
        report.forSchema( schema, checker );
    }

    @Override
    protected void checkNode( RecordStore<NodeRecord> store, NodeRecord node,
                              RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        report.forNode( node, checker );
    }

    @Override
    protected void checkRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel,
                                      RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        report.forRelationship( rel, checker );
    }

    @Override
    protected void checkProperty( RecordStore<PropertyRecord> store, PropertyRecord property,
                                  RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        report.forProperty( property, checker );
    }

    @Override
    protected void checkRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
                                               RelationshipTypeTokenRecord relationshipType,
                                               RecordCheck<RelationshipTypeTokenRecord,
                                                       ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
        report.forRelationshipTypeName( relationshipType, checker );
    }

    @Override
    protected void checkLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord label,
                                    RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport>
                                            checker )
    {
        report.forLabelName( label, checker );
    }

    @Override
    protected void checkPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord key,
                                          RecordCheck<PropertyKeyTokenRecord,
                                          ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
        report.forPropertyKey( key, checker );
    }

    @Override
    protected void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                 RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        report.forDynamicBlock( type, string, checker );
    }

    @Override
    protected void checkDynamicLabel( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                      RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker )
    {
        report.forDynamicLabelBlock( type, string, checker );
    }

    @Override
    protected void checkRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
    {
        report.forRelationshipGroup( record, checker );
    }

    void setSchemaRecordCheck( SchemaRecordCheck schemaRecordCheck )
    {
        this.schemaRecordCheck = schemaRecordCheck;
    }

    @Override
    public void processSchema( RecordStore<DynamicRecord> store, DynamicRecord schema )
    {
        if ( null == schemaRecordCheck )
        {

            super.processSchema( store, schema );
        }
        else
        {
            checkSchema( RecordType.SCHEMA, store, schema, schemaRecordCheck );
        }
    }
}
