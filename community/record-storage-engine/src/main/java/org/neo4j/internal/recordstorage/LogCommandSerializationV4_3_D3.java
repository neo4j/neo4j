/*
 * Copyright (c) "Neo4j"
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

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static org.neo4j.internal.helpers.Numbers.unsignedShortToInt;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.directionFromCombinedKey;
import static org.neo4j.internal.recordstorage.Command.GroupDegreeCommand.groupIdFromCombinedKey;
import static org.neo4j.util.Bits.bitFlag;
import static org.neo4j.util.Bits.bitFlags;

class LogCommandSerializationV4_3_D3 extends LogCommandSerializationV4_2
{
    static final LogCommandSerializationV4_3_D3 INSTANCE = new LogCommandSerializationV4_3_D3();

    @Override
    KernelVersion version()
    {
        return KernelVersion.V4_3_D3;
    }

    @Override
    protected Command readMetaDataCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        MetaDataRecord before = readMetaDataRecord( id, channel );
        MetaDataRecord after = readMetaDataRecord( id, channel );
        return new Command.MetaDataCommand( this, before, after );
    }

    private MetaDataRecord readMetaDataRecord( long id, ReadableChannel channel ) throws IOException
    {
        byte flags = channel.get();
        long value = channel.getLong();
        MetaDataRecord record = new MetaDataRecord();
        record.setId( id );
        if ( bitFlag( flags, Record.IN_USE.byteValue() ) )
        {
            record.initialize( true, value );
        }
        return record;
    }

    @Override
    public void writeMetaDataCommand( WritableChannel channel, Command.MetaDataCommand command ) throws IOException
    {
        channel.put( NeoCommandType.META_DATA_COMMAND );
        channel.putLong( command.getKey() );
        writeMetaDataRecord( channel, command.getBefore() );
        writeMetaDataRecord( channel, command.getAfter() );
    }

    private void writeMetaDataRecord( WritableChannel channel, MetaDataRecord record ) throws IOException
    {
        byte flags = bitFlag( record.inUse(), Record.IN_USE.byteValue() );
        channel.put( flags );
        channel.putLong( record.getValue() );
    }

    @Override
    protected Command readGroupDegreeCommand( ReadableChannel channel ) throws IOException
    {
        long key = channel.getLong();
        long delta = channel.getLong();
        return new Command.GroupDegreeCommand( groupIdFromCombinedKey( key ), directionFromCombinedKey( key ), delta );
    }

    @Override
    public void writeGroupDegreeCommand( WritableChannel channel, Command.GroupDegreeCommand command ) throws IOException
    {
        channel.put( NeoCommandType.UPDATE_GROUP_DEGREE_COMMAND );
        channel.putLong( command.getKey() );
        channel.putLong( command.delta() );
    }

    @Override
    public void writeRelationshipGroupCommand( WritableChannel channel, Command.RelationshipGroupCommand command ) throws IOException
    {
        channel.put( NeoCommandType.REL_GROUP_COMMAND );
        channel.putLong( command.getAfter().getId() );
        writeRelationshipGroupRecord( channel, command.getBefore() );
        writeRelationshipGroupRecord( channel, command.getAfter() );
    }

    private void writeRelationshipGroupRecord( WritableChannel channel, RelationshipGroupRecord record )
            throws IOException
    {
        byte flags = bitFlags( bitFlag( record.inUse(), Record.IN_USE.byteValue() ),
                bitFlag( record.requiresSecondaryUnit(), Record.REQUIRE_SECONDARY_UNIT ),
                bitFlag( record.hasSecondaryUnitId(), Record.HAS_SECONDARY_UNIT ),
                bitFlag( record.isUseFixedReferences(), Record.USES_FIXED_REFERENCE_FORMAT ),
                bitFlag( record.hasExternalDegreesOut(), Record.ADDITIONAL_FLAG_1 ),
                bitFlag( record.hasExternalDegreesIn(), Record.ADDITIONAL_FLAG_2 ),
                bitFlag( record.hasExternalDegreesLoop(), Record.ADDITIONAL_FLAG_3 ) );
        channel.put( flags );
        channel.putShort( (short) record.getType() );
        channel.putLong( record.getNext() );
        channel.putLong( record.getFirstOut() );
        channel.putLong( record.getFirstIn() );
        channel.putLong( record.getFirstLoop() );
        channel.putLong( record.getOwningNode() );
        if ( record.hasSecondaryUnitId() )
        {
            channel.putLong( record.getSecondaryUnitId() );
        }
    }

    @Override
    protected Command readRelationshipGroupCommand( ReadableChannel channel ) throws IOException
    {
        long id = channel.getLong();
        RelationshipGroupRecord before = readRelationshipGroupRecord( id, channel );
        RelationshipGroupRecord after = readRelationshipGroupRecord( id, channel );

        markAfterRecordAsCreatedIfCommandLooksCreated( before, after );
        return new Command.RelationshipGroupCommand( this, before, after );
    }

    private RelationshipGroupRecord readRelationshipGroupRecord( long id, ReadableChannel channel )
            throws IOException
    {
        byte flags = channel.get();
        boolean inUse = bitFlag( flags, Record.IN_USE.byteValue() );
        boolean requireSecondaryUnit = bitFlag( flags, Record.REQUIRE_SECONDARY_UNIT );
        boolean hasSecondaryUnit = bitFlag( flags, Record.HAS_SECONDARY_UNIT );
        boolean usesFixedReferenceFormat = bitFlag( flags, Record.USES_FIXED_REFERENCE_FORMAT );
        boolean hasExternalDegreesOut = bitFlag( flags, Record.ADDITIONAL_FLAG_1 );
        boolean hasExternalDegreesIn = bitFlag( flags, Record.ADDITIONAL_FLAG_2 );
        boolean hasExternalDegreesLoop = bitFlag( flags, Record.ADDITIONAL_FLAG_3 );

        int type = unsignedShortToInt( channel.getShort() );
        long next = channel.getLong();
        long firstOut = channel.getLong();
        long firstIn = channel.getLong();
        long firstLoop = channel.getLong();
        long owningNode = channel.getLong();
        RelationshipGroupRecord record = new RelationshipGroupRecord( id ).initialize( inUse, type, firstOut, firstIn, firstLoop, owningNode, next );
        record.setHasExternalDegreesOut( hasExternalDegreesOut );
        record.setHasExternalDegreesIn( hasExternalDegreesIn );
        record.setHasExternalDegreesLoop( hasExternalDegreesLoop );
        record.setRequiresSecondaryUnit( requireSecondaryUnit );
        if ( hasSecondaryUnit )
        {
            record.setSecondaryUnitIdOnLoad( channel.getLong() );
        }
        record.setUseFixedReferences( usesFixedReferenceFormat );
        return record;
    }
}
