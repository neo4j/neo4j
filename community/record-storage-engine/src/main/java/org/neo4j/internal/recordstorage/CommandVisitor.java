/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.internal.recordstorage.Command.LabelTokenCommand;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.NodeCountsCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.PropertyKeyTokenCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCountsCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipTypeTokenCommand;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;

/**
 * An interface for dealing with commands, either reading or writing them. See also {@link TransactionApplier}. The
 * methods in this class should almost always return false, unless something went wrong.
 */
public interface CommandVisitor
{
    // Store commands
    boolean visitNodeCommand( NodeCommand command ) throws IOException;

    boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException;

    boolean visitPropertyCommand( PropertyCommand command ) throws IOException;

    boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException;

    boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException;

    boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException;

    boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException;

    boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException;

    boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException;

    boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException;

    /**
     * An empty implementation of a {@link CommandVisitor}. Allows you to implement only the methods you are
     * interested in. See also {@link TransactionApplier.Adapter} if need handle commands inside of a transaction, or
     * have a lock.
     */
    class Adapter implements CommandVisitor
    {
        @Override
        public boolean visitNodeCommand( NodeCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipCommand( RelationshipCommand command )
        {
            return false;
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command )
        {
            return false;
        }

        @Override
        public boolean visitLabelTokenCommand( LabelTokenCommand command )
        {
            return false;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command )
        {
            return false;
        }

        @Override
        public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitNodeCountsCommand( NodeCountsCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command )
        {
            return false;
        }
    }
}
