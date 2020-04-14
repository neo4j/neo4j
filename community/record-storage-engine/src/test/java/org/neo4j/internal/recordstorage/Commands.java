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
package org.neo4j.internal.recordstorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.internal.recordstorage.Command.LabelTokenCommand;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.PropertyKeyTokenCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipTypeTokenCommand;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.values.storable.Values;

import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public class Commands
{
    private Commands()
    {
    }

    public static NodeCommand createNode( long id, long... dynamicLabelRecordIds )
    {
        NodeRecord record = new NodeRecord( id );
        record.setInUse( true );
        record.setCreated();
        if ( dynamicLabelRecordIds.length > 0 )
        {
            Collection<DynamicRecord> dynamicRecords = dynamicRecords( dynamicLabelRecordIds );
            record.setLabelField( DynamicNodeLabels.dynamicPointer( dynamicRecords ), dynamicRecords );
        }
        return new NodeCommand( new NodeRecord( id ), record );
    }

    private static List<DynamicRecord> dynamicRecords( long... dynamicLabelRecordIds )
    {
        List<DynamicRecord> dynamicRecords = new ArrayList<>();
        for ( long did : dynamicLabelRecordIds )
        {
            DynamicRecord dynamicRecord = new DynamicRecord( did );
            dynamicRecord.setInUse( true );
            dynamicRecords.add( dynamicRecord );
        }
        return dynamicRecords;
    }

    public static RelationshipCommand createRelationship( long id, long startNode, long endNode, int type )
    {
        RelationshipRecord before = new RelationshipRecord( id );
        before.setInUse( false );
        RelationshipRecord after = new RelationshipRecord( id );
        after.setLinks( startNode, endNode, type );
        after.setInUse( true );
        after.setCreated();
        return new RelationshipCommand( before, after );
    }

    public static LabelTokenCommand createLabelToken( int id, int nameId )
    {
        LabelTokenRecord before = new LabelTokenRecord( id );
        LabelTokenRecord after = new LabelTokenRecord( id );
        populateTokenRecord( after, nameId );
        return new LabelTokenCommand( before, after );
    }

    private static void populateTokenRecord( TokenRecord record, int nameId )
    {
        record.setInUse( true );
        record.setNameId( nameId );
        record.setCreated();
        DynamicRecord dynamicRecord = new DynamicRecord( nameId );
        dynamicRecord.setInUse( true );
        dynamicRecord.setData( new byte[10] );
        dynamicRecord.setCreated();
        record.addNameRecord( dynamicRecord );
    }

    public static PropertyKeyTokenCommand createPropertyKeyToken( int id, int nameId )
    {
        PropertyKeyTokenRecord before = new PropertyKeyTokenRecord( id );
        PropertyKeyTokenRecord after = new PropertyKeyTokenRecord( id );
        populateTokenRecord( after, nameId );
        return new PropertyKeyTokenCommand( before, after );
    }

    public static RelationshipTypeTokenCommand createRelationshipTypeToken( int id, int nameId )
    {
        RelationshipTypeTokenRecord before = new RelationshipTypeTokenRecord( id );
        RelationshipTypeTokenRecord after = new RelationshipTypeTokenRecord( id );
        populateTokenRecord( after, nameId );
        return new RelationshipTypeTokenCommand( before, after );
    }

    public static RelationshipGroupCommand createRelationshipGroup( long id, int type )
    {
        RelationshipGroupRecord before = new RelationshipGroupRecord( id );
        RelationshipGroupRecord after = new RelationshipGroupRecord( id, type );
        after.setInUse( true );
        after.setCreated();
        return new RelationshipGroupCommand( before, after );
    }

    public static SchemaRuleCommand createIndexRule( IndexProviderDescriptor providerDescriptor, long id, LabelSchemaDescriptor descriptor )
    {
        SchemaRule rule = IndexPrototype.forSchema( descriptor, providerDescriptor ).withName( "index_" + id ).materialise( id );
        SchemaRecord before = new SchemaRecord( id ).initialize( false, Record.NO_NEXT_PROPERTY.longValue() );
        SchemaRecord after = new SchemaRecord( id ).initialize( true, 33 );
        return new SchemaRuleCommand( before, after, rule );
    }

    public static PropertyCommand createProperty( long id, PropertyType type, int key,
            long... valueRecordIds )
    {
        PropertyRecord record = new PropertyRecord( id );
        record.setInUse( true );
        record.setCreated();
        PropertyBlock block = new PropertyBlock();
        if ( valueRecordIds.length == 0 )
        {
            PropertyStore.encodeValue( block, key, Values.of( 123 ), null, null, true, NULL );
        }
        else
        {
            PropertyStore.setSingleBlockValue( block, key, type, valueRecordIds[0] );
            block.setValueRecords( dynamicRecords( valueRecordIds ) );
        }
        record.addPropertyBlock( block );
        return new PropertyCommand( new PropertyRecord( id ), record );
    }

    public static CommandsToApply transaction( Command... commands )
    {
        return new GroupOfCommands( commands );
    }
}
