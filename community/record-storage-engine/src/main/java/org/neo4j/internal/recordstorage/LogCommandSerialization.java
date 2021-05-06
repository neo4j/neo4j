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
import org.neo4j.kernel.impl.transaction.log.PositionAwareChannel;
import org.neo4j.storageengine.api.CommandReader;

public abstract class LogCommandSerialization implements CommandReader
{
    abstract KernelVersion version();

    @Override
    public final Command read( ReadableChannel channel ) throws IOException
    {
        byte commandType;
        do
        {
            commandType = channel.get();
        }
        while ( commandType == CommandReader.NONE );

        switch ( commandType )
        {
        case NeoCommandType.NODE_COMMAND:
            return readNodeCommand( channel );
        case NeoCommandType.PROP_COMMAND:
            return readPropertyCommand( channel );
        case NeoCommandType.PROP_INDEX_COMMAND:
            return readPropertyKeyTokenCommand( channel );
        case NeoCommandType.REL_COMMAND:
            return readRelationshipCommand( channel );
        case NeoCommandType.REL_TYPE_COMMAND:
            return readRelationshipTypeTokenCommand( channel );
        case NeoCommandType.LABEL_KEY_COMMAND:
            return readLabelTokenCommand( channel );
        case NeoCommandType.REL_GROUP_COMMAND:
            return readRelationshipGroupCommand( channel );
        case NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND:
            return readRelationshipCountsCommand( channel );
        case NeoCommandType.UPDATE_NODE_COUNTS_COMMAND:
            return readNodeCountsCommand( channel );
        case NeoCommandType.SCHEMA_RULE_COMMAND:
            return readSchemaRuleCommand( channel );
        case NeoCommandType.LEGACY_SCHEMA_RULE_COMMAND:
            return readLegacySchemaRuleCommand( channel );
        case NeoCommandType.NEOSTORE_COMMAND:
            return readNeoStoreCommand( channel );
        case NeoCommandType.META_DATA_COMMAND:
            return readMetaDataCommand( channel );
        case NeoCommandType.UPDATE_GROUP_DEGREE_COMMAND:
            return readGroupDegreeCommand( channel );

        // legacy indexes
        case NeoCommandType.INDEX_DEFINE_COMMAND:
            return readIndexDefineCommand( channel );
        case NeoCommandType.INDEX_ADD_COMMAND:
            return readIndexAddNodeCommand( channel );
        case NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND:
            return readIndexAddRelationshipCommand( channel );
        case NeoCommandType.INDEX_REMOVE_COMMAND:
            return readIndexRemoveCommand( channel );
        case NeoCommandType.INDEX_DELETE_COMMAND:
            return readIndexDeleteCommand( channel );
        case NeoCommandType.INDEX_CREATE_COMMAND:
            return readIndexCreateCommand( channel );
        default:
            throw unknownCommandType( commandType, channel );
        }
    }

    protected Command readNeoStoreCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readMetaDataCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readGroupDegreeCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readSchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readLegacySchemaRuleCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readNodeCountsCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipCountsCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipGroupCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readLabelTokenCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipTypeTokenCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readPropertyKeyTokenCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readPropertyCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readNodeCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexDefineCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexAddNodeCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexAddRelationshipCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexCreateCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexDeleteCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexRemoveCommand( ReadableChannel channel ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeNodeCommand( WritableChannel channel, Command.NodeCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipCommand( WritableChannel channel, Command.RelationshipCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writePropertyCommand( WritableChannel channel, Command.PropertyCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipGroupCommand( WritableChannel channel, Command.RelationshipGroupCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipTypeTokenCommand( WritableChannel channel, Command.RelationshipTypeTokenCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeLabelTokenCommand( WritableChannel channel, Command.LabelTokenCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writePropertyKeyTokenCommand( WritableChannel channel, Command.PropertyKeyTokenCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeSchemaRuleCommand( WritableChannel channel, Command.SchemaRuleCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeNodeCountsCommand( WritableChannel channel, Command.NodeCountsCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipCountsCommand( WritableChannel channel, Command.RelationshipCountsCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeNeoStoreCommand( WritableChannel channel, Command.NeoStoreCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeMetaDataCommand( WritableChannel channel, Command.MetaDataCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    public void writeGroupDegreeCommand( WritableChannel channel, Command.GroupDegreeCommand command ) throws IOException
    {
        throw unsupportedInThisVersionException();
    }

    protected static IOException unknownCommandType( byte commandType, ReadableChannel channel ) throws IOException
    {
        String message = "Unknown command type[" + commandType + "]";
        if ( channel instanceof PositionAwareChannel )
        {
            PositionAwareChannel logChannel = (PositionAwareChannel) channel;
            message += " near " + logChannel.getCurrentPosition();
        }
        return new IOException( message );
    }

    protected IOException unsupportedInThisVersionException()
    {
        return new IOException( "Unsupported in this version: " + getClass().getSimpleName() );
    }
}
