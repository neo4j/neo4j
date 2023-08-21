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

import static org.neo4j.kernel.impl.store.NodeLabelsField.fieldPointsToDynamicRecordOfLabels;

import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;

/**
 * Gathers commands that can result in an index update.
 * <p>
 * This class does not investigate if a relevant index exists, it just gathers
 * commands that have potential to result in changes to index content.
 */
public class IndexUpdatesExtractor extends TransactionApplier.Adapter {
    private final CommandSelector commandSelector;
    private EntityCommandGrouper<NodeCommand> nodeCommands;
    private EntityCommandGrouper<RelationshipCommand> relationshipCommands;
    private boolean hasUpdates;

    public IndexUpdatesExtractor(CommandSelector commandSelector) {
        this.commandSelector = commandSelector;
        nodeCommands = new EntityCommandGrouper<>(NodeCommand.class, 16);
        relationshipCommands = new EntityCommandGrouper<>(RelationshipCommand.class, 16);
    }

    @Override
    public void close() {
        nodeCommands = null;
        relationshipCommands = null;
    }

    @Override
    public boolean visitNodeCommand(NodeCommand command) {
        nodeCommands.add(command);
        if (!hasUpdates && mayResultInIndexUpdates(command)) {
            hasUpdates = true;
        }
        return false;
    }

    @Override
    public boolean visitRelationshipCommand(RelationshipCommand command) {
        relationshipCommands.add(command);
        hasUpdates = true;
        return false;
    }

    private boolean mayResultInIndexUpdates(NodeCommand command) {
        long before = commandSelector.getBefore(command).getLabelField();
        long after = commandSelector.getAfter(command).getLabelField();
        return before != after
                ||
                // Because we don't know here, there may have been changes to a dynamic label record
                // even though it still points to the same one
                fieldPointsToDynamicRecordOfLabels(before)
                || fieldPointsToDynamicRecordOfLabels(after);
    }

    @Override
    public boolean visitPropertyCommand(PropertyCommand command) {
        if (commandSelector.getAfter(command).isNodeSet()) {
            nodeCommands.add(command);
            hasUpdates = true;
        } else if (commandSelector.getAfter(command).isRelSet()) {
            relationshipCommands.add(command);
            hasUpdates = true;
        }
        return false;
    }

    public boolean containsAnyEntityOrPropertyUpdate() {
        return hasUpdates;
    }

    public EntityCommandGrouper<NodeCommand>.Cursor getNodeCommands() {
        return nodeCommands.sortAndAccessGroups();
    }

    public EntityCommandGrouper<RelationshipCommand>.Cursor getRelationshipCommands() {
        return relationshipCommands.sortAndAccessGroups();
    }
}
