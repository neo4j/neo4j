/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.consistency.checking.AbstractStoreProcessor;
import org.neo4j.consistency.checking.CheckDecorator;
import org.neo4j.consistency.checking.RecordCheck;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

class StoreProcessor extends AbstractStoreProcessor
{
    private final ConsistencyReport.Reporter report;

    public StoreProcessor( CheckDecorator decorator, ConsistencyReport.Reporter report )
    {
        super( decorator );
        this.report = report;
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
    protected void checkRelationshipLabel( RecordStore<RelationshipTypeRecord> store, RelationshipTypeRecord label,
                                           RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
    {
        report.forRelationshipLabel( label, checker );
    }

    @Override
    protected void checkPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord key,
                                       RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
    {
        report.forPropertyKey( key, checker );
    }

    @Override
    protected void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                 RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker )
    {
        report.forDynamicBlock( type, string, checker );
    }
}
