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
package org.neo4j.kernel.impl.transaction.command;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.impl.store.DynamicNodeLabels;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordSerializer;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRule;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.api.index.SchemaIndexProvider.NO_INDEX_PROVIDER;

public class Commands
{
    public static NodeCommand createNode( long id, long... dynamicLabelRecordIds )
    {
        NodeCommand command = new NodeCommand();
        NodeRecord record = new NodeRecord( id );
        record.setInUse( true );
        if ( dynamicLabelRecordIds.length > 0 )
        {
            Collection<DynamicRecord> dynamicRecords = dynamicRecords( dynamicLabelRecordIds );
            record.setLabelField( DynamicNodeLabels.dynamicPointer( dynamicRecords ), dynamicRecords );
        }
        command.init( new NodeRecord( id ), record );
        return command;
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
        RelationshipCommand command = new RelationshipCommand();
        RelationshipRecord before = new RelationshipRecord( id );
        before.setInUse( false );
        RelationshipRecord after = new RelationshipRecord( id, startNode, endNode, type );
        after.setInUse( true );
        command.init( before, after );
        return command;
    }

    public static LabelTokenCommand createLabelToken( int id, int nameId )
    {
        LabelTokenCommand command = new LabelTokenCommand();
        LabelTokenRecord record = new LabelTokenRecord( id );
        populateTokenRecord( record, nameId );
        command.init( record );
        return command;
    }

    private static void populateTokenRecord( TokenRecord record, int nameId )
    {
        record.setInUse( true );
        record.setNameId( nameId );
        DynamicRecord dynamicRecord = new DynamicRecord( nameId );
        dynamicRecord.setInUse( true );
        record.addNameRecord( dynamicRecord );
    }

    public static PropertyKeyTokenCommand createPropertyKeyToken( int id, int nameId )
    {
        PropertyKeyTokenCommand command = new PropertyKeyTokenCommand();
        PropertyKeyTokenRecord record = new PropertyKeyTokenRecord( id );
        populateTokenRecord( record, nameId );
        command.init( record );
        return command;
    }

    public static RelationshipTypeTokenCommand createRelationshipTypeToken( int id, int nameId )
    {
        RelationshipTypeTokenCommand command = new RelationshipTypeTokenCommand();
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( id );
        populateTokenRecord( record, nameId );
        command.init( record );
        return command;
    }

    public static RelationshipGroupCommand createRelationshipGroup( long id, int type )
    {
        RelationshipGroupCommand command = new RelationshipGroupCommand();
        RelationshipGroupRecord record = new RelationshipGroupRecord( id, type );
        record.setInUse( true );
        command.init( record );
        return command;
    }

    public static SchemaRuleCommand createIndexRule( long id, int label, int property )
    {
        SchemaRuleCommand command = new SchemaRuleCommand();
        SchemaRule rule = IndexRule.indexRule( id, label, property, NO_INDEX_PROVIDER.getProviderDescriptor() );
        RecordSerializer serializer = new RecordSerializer();
        serializer.append( rule );
        DynamicRecord record = new DynamicRecord( id );
        record.setInUse( true );
        record.setData( serializer.serialize() );
        command.init( Collections.<DynamicRecord>emptyList(), asList( record ), rule );
        return command;
    }

    public static PropertyCommand createProperty( long id, PropertyType type, int key,
            long... valueRecordIds )
    {
        PropertyCommand command = new PropertyCommand();
        PropertyRecord record = new PropertyRecord( id );
        PropertyBlock block = new PropertyBlock();
        if ( valueRecordIds.length == 0 )
        {
            PropertyStore.encodeValue( block, key, 123 /*value*/, null, null );
        }
        else
        {
            PropertyStore.setSingleBlockValue( block, key, type, valueRecordIds[0] );
            block.setValueRecords( dynamicRecords( valueRecordIds ) );
        }
        record.addPropertyBlock( block );
        command.init( new PropertyRecord( id ), record );
        return command;
    }

    public static TransactionRepresentation transactionRepresentation( Command... commands )
    {
        return transactionRepresentation( Arrays.asList( commands ) );
    }

    public static TransactionRepresentation transactionRepresentation( Collection<Command> commands )
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
        tx.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return tx;
    }
}
