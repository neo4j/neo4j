/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.IdType;
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
import org.neo4j.legacy.consistency.report.ConsistencyReport;
import org.neo4j.legacy.consistency.report.ConsistencyReport.NodeConsistencyReport;

import static java.lang.String.format;
import static org.neo4j.legacy.consistency.checking.DynamicStore.ARRAY;
import static org.neo4j.legacy.consistency.checking.DynamicStore.NODE_LABEL;
import static org.neo4j.legacy.consistency.checking.DynamicStore.SCHEMA;

public abstract class AbstractStoreProcessor extends RecordStore.Processor<RuntimeException>
{
    private final RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> sparseNodeChecker;
    private final RecordCheck<NodeRecord, NodeConsistencyReport> denseNodeChecker;
    private final RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> relationshipChecker;
    private final RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> propertyChecker;
    private final RecordCheck<PropertyKeyTokenRecord, ConsistencyReport.PropertyKeyTokenConsistencyReport> propertyKeyTokenChecker;
    private final RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> relationshipTypeTokenChecker;
    private final RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> labelTokenChecker;
    private final RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> relationshipGroupChecker;

    public AbstractStoreProcessor()
    {
        this( CheckDecorator.NONE );
    }

    public AbstractStoreProcessor( CheckDecorator decorator )
    {
        this.sparseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forSparseNodes() );
        this.denseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forDenseNodes() );
        this.relationshipChecker = decorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        this.propertyChecker = decorator.decoratePropertyChecker( new PropertyRecordCheck() );
        this.propertyKeyTokenChecker = decorator.decoratePropertyKeyTokenChecker( new PropertyKeyTokenRecordCheck() );
        this.relationshipTypeTokenChecker = decorator.decorateRelationshipTypeTokenChecker( new
                RelationshipTypeTokenRecordCheck() );
        this.labelTokenChecker = decorator.decorateLabelTokenChecker( new LabelTokenRecordCheck() );
        this.relationshipGroupChecker = decorator.decorateRelationshipGroupChecker( new RelationshipGroupRecordCheck() );
    }

    protected abstract void checkNode(
            RecordStore<NodeRecord> store, NodeRecord node,
            RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker );

    protected abstract void checkRelationship(
            RecordStore<RelationshipRecord> store, RelationshipRecord rel,
            RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker );

    protected abstract void checkProperty(
            RecordStore<PropertyRecord> store, PropertyRecord property,
            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker );

    protected abstract void checkRelationshipTypeToken(
            RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record,
            RecordCheck<RelationshipTypeTokenRecord, ConsistencyReport.RelationshipTypeConsistencyReport> checker );

    protected abstract void checkLabelToken(
            RecordStore<LabelTokenRecord> store,
            LabelTokenRecord record,
            RecordCheck<LabelTokenRecord, ConsistencyReport.LabelTokenConsistencyReport> checker );

    protected abstract void checkPropertyKeyToken(
            RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record,
            RecordCheck<PropertyKeyTokenRecord,
                    ConsistencyReport.PropertyKeyTokenConsistencyReport> checker );

    protected abstract void checkDynamic(
            RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker );

    protected abstract void checkDynamicLabel(
            RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord, ConsistencyReport.DynamicLabelConsistencyReport> checker );

    protected abstract void checkRelationshipGroup(
            RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, ConsistencyReport.RelationshipGroupConsistencyReport> checker );

    @Override
    public void processSchema( RecordStore<DynamicRecord> store, DynamicRecord schema )
    {
        // cf. StoreProcessor
        checkDynamic( RecordType.SCHEMA, store, schema, new DynamicRecordCheck( store, SCHEMA ) );
    }

    @Override
    public final void processNode( RecordStore<NodeRecord> store, NodeRecord node )
    {
        if ( node.isDense() )
        {
            checkNode( store, node, denseNodeChecker );
        }
        else
        {
            checkNode( store, node, sparseNodeChecker );
        }
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
        if ( IdType.STRING_BLOCK.equals( idType ) )
        {
            type = RecordType.STRING_PROPERTY;
            dereference = DynamicStore.STRING;
        }
        else if ( IdType.RELATIONSHIP_TYPE_TOKEN_NAME.equals( idType ) )
        {
            type = RecordType.RELATIONSHIP_TYPE_NAME;
            dereference = DynamicStore.RELATIONSHIP_TYPE;
        }
        else if ( IdType.PROPERTY_KEY_TOKEN_NAME.equals( idType ) )
        {
            type = RecordType.PROPERTY_KEY_NAME;
            dereference = DynamicStore.PROPERTY_KEY;
        }
        else if ( IdType.LABEL_TOKEN_NAME.equals( idType ) )
        {
            type = RecordType.LABEL_NAME;
            dereference = DynamicStore.LABEL;
        }
        else
        {
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
    public final void processLabelArrayWithOwner( RecordStore<DynamicRecord> store, DynamicRecord array )
    {
        checkDynamic( RecordType.NODE_DYNAMIC_LABEL, store, array, new DynamicRecordCheck( store, NODE_LABEL ) );
        checkDynamicLabel( RecordType.NODE_DYNAMIC_LABEL, store, array, new NodeDynamicLabelOrphanChainStartCheck() );
    }

    @Override
    public final void processRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
                                                    RelationshipTypeTokenRecord record )
    {
        checkRelationshipTypeToken( store, record, relationshipTypeTokenChecker );
    }

    @Override
    public final void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store,
                                               PropertyKeyTokenRecord record )
    {
        checkPropertyKeyToken( store, record, propertyKeyTokenChecker );
    }

    @Override
    public void processLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record )
    {
        checkLabelToken( store, record, labelTokenChecker );
    }

    @Override
    public void processRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record )
            throws RuntimeException
    {
        checkRelationshipGroup( store, record, relationshipGroupChecker );
    }
}
