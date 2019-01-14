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
package org.neo4j.kernel.impl.api;


import java.io.IOException;

import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;

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

    boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException;

    // Index commands
    boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException;

    boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException;

    boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException;

    boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException;

    boolean visitIndexCreateCommand( CreateCommand command ) throws IOException;

    boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException;

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
        public boolean visitNodeCommand( NodeCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
        {
            return false;
        }
    }

    /**
     * Wraps a given {@link CommandVisitor}, allowing you to do some extra operations before/after/instead of the
     * delegate executes.
     */
    class Delegator implements CommandVisitor
    {
        private final CommandVisitor delegate;

        public Delegator( CommandVisitor delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public boolean visitNodeCommand( NodeCommand command ) throws IOException
        {
            return delegate.visitNodeCommand( command );
        }

        @Override
        public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
        {
            return delegate.visitRelationshipCommand( command );
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
        {
            return delegate.visitPropertyCommand( command );
        }

        @Override
        public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
        {
            return delegate.visitRelationshipGroupCommand( command );
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
        {
            return delegate.visitRelationshipTypeTokenCommand( command );
        }

        @Override
        public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
        {
            return delegate.visitLabelTokenCommand( command );
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
        {
            return delegate.visitPropertyKeyTokenCommand( command );
        }

        @Override
        public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
        {
            return delegate.visitSchemaRuleCommand( command );
        }

        @Override
        public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
        {
            return delegate.visitNeoStoreCommand( command );
        }

        @Override
        public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
        {
            return delegate.visitIndexAddNodeCommand( command );
        }

        @Override
        public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
        {
            return delegate.visitIndexAddRelationshipCommand( command );
        }

        @Override
        public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
        {
            return delegate.visitIndexRemoveCommand( command );
        }

        @Override
        public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
        {
            return delegate.visitIndexDeleteCommand( command );
        }

        @Override
        public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
        {
            return delegate.visitIndexCreateCommand( command );
        }

        @Override
        public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
        {
            return delegate.visitIndexDefineCommand( command );
        }

        @Override
        public boolean visitNodeCountsCommand( NodeCountsCommand command ) throws IOException
        {
            return delegate.visitNodeCountsCommand( command );
        }

        @Override
        public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
        {
            return delegate.visitRelationshipCountsCommand( command );
        }
    }
}
