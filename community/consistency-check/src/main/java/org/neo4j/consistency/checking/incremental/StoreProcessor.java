/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.consistency.checking.incremental;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.checking.AbstractStoreProcessor;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.DynamicLabelConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

class StoreProcessor extends AbstractStoreProcessor
{
    private final ConsistencyReport.Reporter report;

    StoreProcessor( ConsistencyReport.Reporter report )
    {
        this.report = report;
    }

    @Override
    protected void checkNode( RecordStore<NodeRecord> store, NodeRecord node,
                              RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
    {
        report.forNodeChange( store.forceGetRaw( node ), node, checker );
    }

    @Override
    protected void checkRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel,
                                      RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
    {
        report.forRelationshipChange( store.forceGetRaw( rel ), rel, checker );
    }

    @Override
    protected void checkProperty( RecordStore<PropertyRecord> store, PropertyRecord property,
                                  RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
    {
        report.forPropertyChange( store.forceGetRaw( property ), property, checker );
    }

    @Override
    protected void checkRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
                                               RelationshipTypeTokenRecord record,
                                               RecordCheck<RelationshipTypeTokenRecord,
                                                       ConsistencyReport.RelationshipTypeConsistencyReport> checker )
    {
        report.forRelationshipTypeNameChange( store.forceGetRaw( record ), record, checker );
    }

    @Override
    protected void checkLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record,
                                    RecordCheck<LabelTokenRecord,
                                            ConsistencyReport.LabelTokenConsistencyReport> checker )
    {
        report.forLabelNameChange( store.forceGetRaw( record ), record, checker );
    }

    @Override
    protected void checkPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record,
                                          RecordCheck<PropertyKeyTokenRecord,
                                                  ConsistencyReport.PropertyKeyTokenConsistencyReport> checker )
    {
        report.forPropertyKeyChange( store.forceGetRaw( record ), record, checker );
    }

    @Override
    protected void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                 RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        report.forDynamicBlockChange( type, store.forceGetRaw( string ), string, checker );
    }


    @Override
    protected void checkDynamicLabel( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                      RecordCheck<DynamicRecord, DynamicLabelConsistencyReport> checker )
    {
        report.forDynamicLabelBlockChange( type, store.forceGetRaw( string ), string, checker );
    }

    @Override
    protected void checkRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker )
    {
        report.forRelationshipGroupChange( store.forceGetRaw( record ), record, checker );
    }
}
