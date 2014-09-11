/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;

/**
 * A NeoCommandHandler has to handle all type of commands that a Neo transaction can generate. It provides
 * methods for handling each type of Command available through a callback pattern to avoid dynamic dispatching.
 * Implementations need to provide all these methods of course, but it is expected that they will delegate
 * the actual work to implementations that hold related functionality together, using a Facade pattern.
 * For example, it is conceivable that a CommandWriterHandler would use a NeoCommandHandler and a SchemaCommandHandler.
 *
 * The order in which the methods of a NeoCommandHandler is expected to be called is this:
 * <ol>
 * <li>zero or more calls to visit??? methods</li>
 * <li>{@link #apply()}</li>
 * <li>{@link #close()}</li>
 * </ol>
 */
public interface NeoCommandHandler extends AutoCloseable
{
    // NeoStore commands
    boolean visitNodeCommand( Command.NodeCommand command ) throws IOException;
    boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException;
    boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException;
    boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException;
    boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException;
    boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException;
    boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException;
    boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException;
    boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException;

    // Index commands
    boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException;
    boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException;
    boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException;
    boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException;
    boolean visitIndexCreateCommand( CreateCommand command ) throws IOException;
    boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException;
    
    /**
     * Applies pending changes that might have been accumulated when visiting the commands.
     * A command handler can expect a call to {@link #apply()} before {@link #close()}.
     */
    void apply();

    /**
     * Closes any resources acquired by this handler.
     */
    @Override
    void close();

    public static class Adapter implements NeoCommandHandler
    {
        @Override
        public boolean visitNodeCommand( NodeCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command )
                throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
        {
            return true;
        }

        @Override
        public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
        {
            return true;
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

    public static class Delegator implements NeoCommandHandler
    {
        private final NeoCommandHandler delegate;

        public Delegator( NeoCommandHandler delegate )
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

    public static class HandlerVisitor implements Visitor<Command, IOException>
    {
        private final NeoCommandHandler handler;

        public HandlerVisitor( NeoCommandHandler handler )
        {
            this.handler = handler;
        }

        @Override
        public boolean visit( Command element ) throws IOException
        {
            element.handle( handler );
            return true;
        }
    }
}
