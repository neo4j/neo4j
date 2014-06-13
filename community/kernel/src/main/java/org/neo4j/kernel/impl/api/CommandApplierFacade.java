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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexCommand.CreateCommand;
import org.neo4j.kernel.impl.index.IndexCommand.DeleteCommand;
import org.neo4j.kernel.impl.index.IndexCommand.RemoveCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NeoStoreCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;

public class CommandApplierFacade implements NeoCommandHandler, Visitor<Command, IOException>
{
    private final NeoCommandHandler storeApplier;
    private final NeoCommandHandler indexApplier;
    private final NeoCommandHandler legacyIndexApplier;

    public CommandApplierFacade( NeoCommandHandler storeApplier, NeoCommandHandler indexApplier,
            NeoCommandHandler legacyIndexApplier )
    {
        this.storeApplier = storeApplier;
        this.indexApplier = indexApplier;
        this.legacyIndexApplier = legacyIndexApplier;
    }

    @Override
    public void close()
    {
        storeApplier.close();
        indexApplier.close();
        legacyIndexApplier.close();
    }

    @Override
    public boolean visit( Command element ) throws IOException
    {
        element.handle( this );
        return true;
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        storeApplier.visitNodeCommand( command );
        indexApplier.visitNodeCommand( command );
        return true;
    }

    @Override
    public boolean visitRelationshipCommand( RelationshipCommand command ) throws IOException
    {
        storeApplier.visitRelationshipCommand( command );
        return true;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        storeApplier.visitPropertyCommand( command );
        indexApplier.visitPropertyCommand( command );
        return true;
    }

    @Override
    public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command ) throws IOException
    {
        return storeApplier.visitRelationshipGroupCommand( command );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command ) throws IOException
    {
        return storeApplier.visitRelationshipTypeTokenCommand( command );
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command ) throws IOException
    {
        return storeApplier.visitPropertyKeyTokenCommand( command );
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
    {
        return storeApplier.visitSchemaRuleCommand( command );
    }

    @Override
    public boolean visitNeoStoreCommand( NeoStoreCommand command ) throws IOException
    {
        return storeApplier.visitNeoStoreCommand( command );
    }

    @Override
    public boolean visitLabelTokenCommand( LabelTokenCommand command ) throws IOException
    {
        return storeApplier.visitLabelTokenCommand( command );
    }

    @Override
    public boolean visitIndexRemoveCommand( RemoveCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexRemoveCommand( command );
    }

    @Override
    public boolean visitIndexAddNodeCommand( AddNodeCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexAddNodeCommand( command );
    }

    @Override
    public boolean visitIndexAddRelationshipCommand( AddRelationshipCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexAddRelationshipCommand( command );
    }

    @Override
    public boolean visitIndexDeleteCommand( DeleteCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexDeleteCommand( command );
    }

    @Override
    public boolean visitIndexCreateCommand( CreateCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexCreateCommand( command );
    }

    @Override
    public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
    {
        return legacyIndexApplier.visitIndexDefineCommand( command );
    }
}
