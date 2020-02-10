/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.LabelTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.NodeConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.PropertyKeyTokenConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipGroupConsistencyReport;
import org.neo4j.consistency.report.ConsistencyReport.RelationshipTypeConsistencyReport;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;

import static java.lang.String.format;
import static org.neo4j.consistency.checking.DynamicStore.ARRAY;
import static org.neo4j.consistency.checking.DynamicStore.NODE_LABEL;

public abstract class AbstractStoreProcessor extends RecordStore.Processor<RuntimeException>
{
    private RecordCheck<NodeRecord, NodeConsistencyReport> sparseNodeChecker;
    private RecordCheck<NodeRecord, NodeConsistencyReport> denseNodeChecker;
    private RecordCheck<RelationshipRecord, RelationshipConsistencyReport> relationshipChecker;
    private final RecordCheck<PropertyRecord, PropertyConsistencyReport> propertyChecker;
    private final RecordCheck<PropertyKeyTokenRecord, PropertyKeyTokenConsistencyReport> propertyKeyTokenChecker;
    private final RecordCheck<RelationshipTypeTokenRecord,RelationshipTypeConsistencyReport> relationshipTypeTokenChecker;
    private final RecordCheck<LabelTokenRecord,LabelTokenConsistencyReport> labelTokenChecker;
    private final RecordCheck<RelationshipGroupRecord,RelationshipGroupConsistencyReport> relationshipGroupChecker;

    public AbstractStoreProcessor( CheckDecorator decorator )
    {
        this.sparseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forSparseNodes() );
        this.denseNodeChecker = decorator.decorateNodeChecker( NodeRecordCheck.forDenseNodes() );
        this.relationshipChecker = decorator.decorateRelationshipChecker( new RelationshipRecordCheck() );
        this.propertyChecker = decorator.decoratePropertyChecker( new PropertyRecordCheck() );
        this.propertyKeyTokenChecker = decorator.decoratePropertyKeyTokenChecker( new PropertyKeyTokenRecordCheck() );
        this.relationshipTypeTokenChecker =
                decorator.decorateRelationshipTypeTokenChecker( new RelationshipTypeTokenRecordCheck() );
        this.labelTokenChecker = decorator.decorateLabelTokenChecker( new LabelTokenRecordCheck() );
        this.relationshipGroupChecker = decorator.decorateRelationshipGroupChecker( new RelationshipGroupRecordCheck() );
    }

    public void reDecorateRelationship( CheckDecorator decorator, RelationshipRecordCheck newChecker )
    {
        this.relationshipChecker = decorator.decorateRelationshipChecker( newChecker );
    }

    public void reDecorateNode( CheckDecorator decorator, NodeRecordCheck newChecker, boolean sparseNode )
    {
        if ( sparseNode )
        {
            this.sparseNodeChecker = decorator.decorateNodeChecker( newChecker );
        }
        else
        {
            this.denseNodeChecker = decorator.decorateNodeChecker( newChecker );
        }
    }

    protected abstract void checkNode(
            RecordStore<NodeRecord> store, NodeRecord node,
            RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkRelationship(
            RecordStore<RelationshipRecord> store, RelationshipRecord rel,
            RecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkProperty(
            RecordStore<PropertyRecord> store, PropertyRecord property,
            RecordCheck<PropertyRecord, PropertyConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkRelationshipTypeToken(
            RecordStore<RelationshipTypeTokenRecord> store,
            RelationshipTypeTokenRecord record,
            RecordCheck<RelationshipTypeTokenRecord, RelationshipTypeConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkLabelToken(
            RecordStore<LabelTokenRecord> store,
            LabelTokenRecord record,
            RecordCheck<LabelTokenRecord, LabelTokenConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkPropertyKeyToken(
            RecordStore<PropertyKeyTokenRecord> store, PropertyKeyTokenRecord record,
            RecordCheck<PropertyKeyTokenRecord,
                    PropertyKeyTokenConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkDynamic(
            RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord, ConsistencyReport.DynamicConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkDynamicLabel(
            RecordType type, RecordStore<DynamicRecord> store, DynamicRecord string,
            RecordCheck<DynamicRecord, ConsistencyReport.DynamicLabelConsistencyReport> checker, PageCursorTracer cursorTracer );

    protected abstract void checkRelationshipGroup(
            RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record,
            RecordCheck<RelationshipGroupRecord, RelationshipGroupConsistencyReport> checker, PageCursorTracer cursorTracer );

    @Override
    public void processSchema( RecordStore<SchemaRecord> store, SchemaRecord schema, PageCursorTracer cursorTracer )
    {
        // See StoreProcessor
    }

    @Override
    public void processNode( RecordStore<NodeRecord> store, NodeRecord node, PageCursorTracer cursorTracer )
    {
        if ( node.isDense() )
        {
            checkNode( store, node, denseNodeChecker, cursorTracer );
        }
        else
        {
            checkNode( store, node, sparseNodeChecker, cursorTracer );
        }
    }

    @Override
    public final void processRelationship( RecordStore<RelationshipRecord> store, RelationshipRecord rel, PageCursorTracer cursorTracer )
    {
        checkRelationship( store, rel, relationshipChecker, cursorTracer );
    }

    @Override
    public final void processProperty( RecordStore<PropertyRecord> store, PropertyRecord property, PageCursorTracer cursorTracer )
    {
        checkProperty( store, property, propertyChecker, cursorTracer );
    }

    @Override
    public final void processString( RecordStore<DynamicRecord> store, DynamicRecord string, IdType idType, PageCursorTracer cursorTracer )
    {
        RecordType type;
        DynamicStore dereference;
        switch ( idType )
        {
        case STRING_BLOCK:
            type = RecordType.STRING_PROPERTY;
            dereference = DynamicStore.STRING;
            break;
        case RELATIONSHIP_TYPE_TOKEN_NAME:
            type = RecordType.RELATIONSHIP_TYPE_NAME;
            dereference = DynamicStore.RELATIONSHIP_TYPE;
            break;
        case PROPERTY_KEY_TOKEN_NAME:
            type = RecordType.PROPERTY_KEY_NAME;
            dereference = DynamicStore.PROPERTY_KEY;
            break;
        case LABEL_TOKEN_NAME:
            type = RecordType.LABEL_NAME;
            dereference = DynamicStore.LABEL;
            break;
        default:
            throw new IllegalArgumentException( format( "The id type [%s] is not valid for String records.", idType ) );
        }
        checkDynamic( type, store, string, new DynamicRecordCheck( store, dereference ), cursorTracer );
    }

    @Override
    public final void processArray( RecordStore<DynamicRecord> store, DynamicRecord array, PageCursorTracer cursorTracer )
    {
        checkDynamic( RecordType.ARRAY_PROPERTY, store, array, new DynamicRecordCheck( store, ARRAY ), cursorTracer );
    }

    @Override
    public final void processLabelArrayWithOwner( RecordStore<DynamicRecord> store, DynamicRecord array, PageCursorTracer cursorTracer )
    {
        checkDynamic( RecordType.NODE_DYNAMIC_LABEL, store, array, new DynamicRecordCheck( store, NODE_LABEL ), cursorTracer );
        checkDynamicLabel( RecordType.NODE_DYNAMIC_LABEL, store, array, new NodeDynamicLabelOrphanChainStartCheck(), cursorTracer );
    }

    @Override
    public final void processRelationshipTypeToken( RecordStore<RelationshipTypeTokenRecord> store,
                                                    RelationshipTypeTokenRecord record, PageCursorTracer cursorTracer )
    {
        checkRelationshipTypeToken( store, record, relationshipTypeTokenChecker, cursorTracer );
    }

    @Override
    public final void processPropertyKeyToken( RecordStore<PropertyKeyTokenRecord> store,
                                               PropertyKeyTokenRecord record, PageCursorTracer cursorTracer )
    {
        checkPropertyKeyToken( store, record, propertyKeyTokenChecker, cursorTracer );
    }

    @Override
    public void processLabelToken( RecordStore<LabelTokenRecord> store, LabelTokenRecord record, PageCursorTracer cursorTracer )
    {
        checkLabelToken( store, record, labelTokenChecker, cursorTracer );
    }

    @Override
    public void processRelationshipGroup( RecordStore<RelationshipGroupRecord> store, RelationshipGroupRecord record, PageCursorTracer cursorTracer )
            throws RuntimeException
    {
        checkRelationshipGroup( store, record, relationshipGroupChecker, cursorTracer );
    }

}
