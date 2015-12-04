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

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.LockGroup;
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
import org.neo4j.kernel.impl.transaction.log.CommandWriter;

/**
 * A CommandHandler has to handle all type of commands that a Neo4j transaction can generate. It provides
 * methods for handling each type of {@link Command} available through a callback pattern to avoid dynamic dispatching.
 * Implementations need to provide all these methods of course, but it is expected that they will delegate
 * the actual work to implementations that hold related functionality together, using a Facade pattern.
 * For example, it is conceivable that a {@link CommandWriter} would use a {@link CommandHandler}.
 * <p>
 * A CommandHandler must also be capable of visiting commands for a batch of transactions, the access pattern
 * goes like this:
 * <ol>
 * <li>{@link #begin(TransactionToApply)} called for the transaction to now visit commands for</li>
 * <li>...one or more commands for that transaction</li>
 * <li>{@link #end()}</li>
 * <li>{@link #begin(TransactionToApply)} called for the next transaction in the same batch</li>
 * <li>...one or more commands for that transaction</li>
 * <li>{@link #end()}</li>
 * <li>...same as above for every transaction in this batch</li>
 * <li>{@link #apply()}</li>
 * <li>{@link #close()}</li>
 * </ol>
 * The boolean returned from visit methods is false for continuing traversal, and true for breaking traversal.
 */
public interface CommandHandler extends AutoCloseable
{
    CommandHandler EMPTY = new CommandHandler.Adapter();

    /**
     * Called before each transaction in this batch.
     */
    void begin( TransactionToApply transaction, LockGroup locks ) throws IOException;

    /**
     * Called after each transaction in this batch.
     */
    void end() throws Exception;

    // Store commands
    boolean visitNodeCommand( Command.NodeCommand command ) throws IOException;

    boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException;

    boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException;

    boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException;

    boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException;

    boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException;

    boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException;

    boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException;

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
     * Applies pending changes that might have been accumulated when visiting the commands for all
     * transaction in this batch. This method is called before {@link #close()} and after all commands
     * for all transactions have been visited, also after all {@link #end()} calls.
     * A command handler can expect a call to {@link #apply()} before {@link #close()}.
     */
    void apply();

    /**
     * Closes any resources acquired by this handler.
     */
    @Override
    void close();

    class Adapter implements CommandHandler
    {
        @Override
        public void begin( TransactionToApply transaction, LockGroup locks ) throws IOException
        {
        }

        @Override
        public void end() throws Exception
        {
        }

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
        public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command )
                throws IOException
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

        @Override
        public void apply()
        {
        }

        @Override
        public void close()
        {
        }
    }

    class Delegator implements CommandHandler
    {
        private final CommandHandler delegate;

        public Delegator( CommandHandler delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void begin( TransactionToApply transaction, LockGroup locks ) throws IOException
        {
            delegate.begin( transaction, locks );
        }

        @Override
        public void end() throws Exception
        {
            delegate.end();
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

        @Override
        public void apply()
        {
            delegate.apply();
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }

    class HandlerVisitor implements Visitor<Command,IOException>
    {
        private final CommandHandler handler;

        public HandlerVisitor( CommandHandler handler )
        {
            this.handler = handler;
        }

        @Override
        public boolean visit( Command element ) throws IOException
        {
            element.handle( handler );
            return false;
        }
    }
}
