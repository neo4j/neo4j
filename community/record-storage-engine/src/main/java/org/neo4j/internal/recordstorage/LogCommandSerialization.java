/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.impl.transaction.log.LogPositionAwareChannel;
import org.neo4j.storageengine.api.BaseCommandReader;

public abstract class LogCommandSerialization extends BaseCommandReader {

    @Override
    public final Command read(byte commandType, ReadableChannel channel) throws IOException {
        return switch (commandType) {
            case NeoCommandType.NODE_COMMAND -> readNodeCommand(channel);
            case NeoCommandType.CREATE_NODE_COMMAND -> readCreatedNodeCommand(channel);
            case NeoCommandType.DELETE_NODE_COMMAND -> readDeletedNodeCommand(channel);

            case NeoCommandType.PROP_COMMAND -> readPropertyCommand(channel);
            case NeoCommandType.CREATE_PROP_COMMAND -> readCreatedPropertyCommand(channel);
            case NeoCommandType.DELETE_PROP_COMMAND -> readDeletedPropertyCommand(channel);

            case NeoCommandType.PROP_INDEX_COMMAND -> readPropertyKeyTokenCommand(channel);

            case NeoCommandType.REL_COMMAND -> readRelationshipCommand(channel);
            case NeoCommandType.CREATE_REL_COMMAND -> readCreatedRelationshipCommand(channel);
            case NeoCommandType.DELETE_REL_COMMAND -> readDeletedRelationshipCommand(channel);

            case NeoCommandType.REL_TYPE_COMMAND -> readRelationshipTypeTokenCommand(channel);
            case NeoCommandType.LABEL_KEY_COMMAND -> readLabelTokenCommand(channel);
            case NeoCommandType.REL_GROUP_COMMAND -> readRelationshipGroupCommand(channel);
            case NeoCommandType.REL_GROUP_EXTENDED_COMMAND -> readRelationshipGroupExtendedCommand(channel);
            case NeoCommandType.UPDATE_RELATIONSHIP_COUNTS_COMMAND -> readRelationshipCountsCommand(channel);
            case NeoCommandType.UPDATE_NODE_COUNTS_COMMAND -> readNodeCountsCommand(channel);
            case NeoCommandType.SCHEMA_RULE_COMMAND -> readSchemaRuleCommand(channel);
            case NeoCommandType.LEGACY_SCHEMA_RULE_COMMAND -> readLegacySchemaRuleCommand(channel);
            case NeoCommandType.NEOSTORE_COMMAND -> readNeoStoreCommand(channel);
            case NeoCommandType.META_DATA_COMMAND -> readMetaDataCommand(channel);
            case NeoCommandType.UPDATE_GROUP_DEGREE_COMMAND -> readGroupDegreeCommand(channel);
            case NeoCommandType.ENRICHMENT_COMMAND -> readEnrichmentCommand(channel);

                // legacy indexes
            case NeoCommandType.INDEX_DEFINE_COMMAND -> readIndexDefineCommand(channel);
            case NeoCommandType.INDEX_ADD_COMMAND -> readIndexAddNodeCommand(channel);
            case NeoCommandType.INDEX_ADD_RELATIONSHIP_COMMAND -> readIndexAddRelationshipCommand(channel);
            case NeoCommandType.INDEX_REMOVE_COMMAND -> readIndexRemoveCommand(channel);
            case NeoCommandType.INDEX_DELETE_COMMAND -> readIndexDeleteCommand(channel);
            case NeoCommandType.INDEX_CREATE_COMMAND -> readIndexCreateCommand(channel);
            default -> throw unknownCommandType(commandType, channel);
        };
    }

    protected Command readNeoStoreCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readMetaDataCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readGroupDegreeCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readSchemaRuleCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readLegacySchemaRuleCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readNodeCountsCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipCountsCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipGroupCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipGroupExtendedCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readLabelTokenCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipTypeTokenCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readRelationshipCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readCreatedRelationshipCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readDeletedRelationshipCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readPropertyKeyTokenCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readPropertyCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readDeletedPropertyCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readCreatedPropertyCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readNodeCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readDeletedNodeCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readCreatedNodeCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexDefineCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexAddNodeCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexAddRelationshipCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexCreateCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexDeleteCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readIndexRemoveCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected Command readEnrichmentCommand(ReadableChannel channel) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeDeletedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeCreatedNodeCommand(WritableChannel channel, Command.NodeCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeCreatedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeDeletedRelationshipCommand(WritableChannel channel, Command.RelationshipCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writePropertyCommand(WritableChannel channel, Command.PropertyCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeDeletedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeCreatedPropertyCommand(WritableChannel channel, Command.PropertyCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipGroupCommand(WritableChannel channel, Command.RelationshipGroupCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipTypeTokenCommand(WritableChannel channel, Command.RelationshipTypeTokenCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeLabelTokenCommand(WritableChannel channel, Command.LabelTokenCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writePropertyKeyTokenCommand(WritableChannel channel, Command.PropertyKeyTokenCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeSchemaRuleCommand(WritableChannel channel, Command.SchemaRuleCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeNodeCountsCommand(WritableChannel channel, Command.NodeCountsCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeRelationshipCountsCommand(WritableChannel channel, Command.RelationshipCountsCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeMetaDataCommand(WritableChannel channel, Command.MetaDataCommand command) throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeGroupDegreeCommand(WritableChannel channel, Command.GroupDegreeCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    public void writeEnrichmentCommand(WritableChannel channel, Command.RecordEnrichmentCommand command)
            throws IOException {
        throw unsupportedInThisVersionException();
    }

    protected static IOException unknownCommandType(byte commandType, ReadableChannel channel) throws IOException {
        String message = "Unknown command type[" + commandType + "]";
        if (channel instanceof LogPositionAwareChannel logChannel) {
            message += " near " + logChannel.getCurrentLogPosition();
        }
        return new IOException(message);
    }

    protected IOException unsupportedInThisVersionException() {
        return new IOException("Unsupported in this version: " + getClass().getSimpleName());
    }
}
