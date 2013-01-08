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

import static java.lang.String.format;
import static org.neo4j.consistency.checking.DynamicStore.ARRAY;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public abstract class AbstractStoreProcessor extends RecordStore.Processor
{
    private final RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> neoStoreChecker;
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> nodeChecker;
    private final RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker;
    private final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyChecker;
    private final RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> propertyKeyChecker;
    private final RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> relationshipLabelChecker;

    public AbstractStoreProcessor()
    {
        this( CheckDecorator.NONE );
    }

    public AbstractStoreProcessor( CheckDecorator decorator )
    {
        this.neoStoreChecker = decorator.decorateNeoStoreChecker( new NeoStoreCheck() );
        this.nodeChecker = decorator.decorateNodeChecker( new NodeRecordCheck() );
        this.relationshipChecker = decorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        this.propertyChecker = decorator.decoratePropertyChecker( new PropertyRecordCheck() );
        this.propertyKeyChecker = decorator.decoratePropertyKeyChecker( new PropertyKeyRecordCheck() );
        this.relationshipLabelChecker = decorator.decorateLabelChecker( new RelationshipLabelRecordCheck() );
    }

    protected abstract void checkNode( RecordStore<NodeRecord> store, NodeRecord node,
                                       RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker );

    protected abstract void checkRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel,
                                               RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker );

    protected abstract void checkProperty( RecordStore<PropertyRecord> store, PropertyRecord property,
                                           RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker );

    protected abstract void checkRelationshipLabel( RecordStore<RelationshipTypeRecord> store,
                                                    RelationshipTypeRecord record,
                                                    RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker );

    protected abstract void checkPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord record,
                                                RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker );

    protected abstract void checkDynamic( RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
                                          RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker );

    @Override
    public final void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        checkNode( store, node, nodeChecker );
    }

    @Override
    public final void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel )
    {
        checkRelationship( store, rel, relationshipChecker );
    }

    @Override
    public final void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property )
    {
        checkProperty( store, property, propertyChecker );
    }

    @Override
    public final void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType )
    {
        RecordType type;
        DynamicStore dereference;
        switch ( idType )
        {
        case STRING_BLOCK:
            type = RecordType.STRING_PROPERTY;
            dereference = DynamicStore.STRING;
            break;
        case RELATIONSHIP_TYPE_BLOCK:
            type = RecordType.RELATIONSHIP_LABEL_NAME;
            dereference = DynamicStore.RELATIONSHIP_LABEL;
            break;
        case PROPERTY_INDEX_BLOCK:
            type = RecordType.PROPERTY_KEY_NAME;
            dereference = DynamicStore.PROPERTY_KEY;
            break;
        default:
            throw new IllegalArgumentException( format( "The id type [%s] is not valid for String records.", idType ) );
        }
        checkDynamic( type, store, string, new DynamicRecordCheck( store, dereference ) );
    }

    @Override
    public final void processArray( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        checkDynamic( RecordType.ARRAY_PROPERTY, store, array, new DynamicRecordCheck( store, ARRAY ) );
    }

    @Override
    public final void processRelationshipType( RecordStore<RelationshipTypeRecord> store,
                                               RelationshipTypeRecord record )
    {
        checkRelationshipLabel( store, record, relationshipLabelChecker );
    }

    @Override
    public final void processPropertyIndex( RecordStore<PropertyIndexRecord> store, PropertyIndexRecord record )
    {
        checkPropertyIndex( store, record, propertyKeyChecker );
    }
}
