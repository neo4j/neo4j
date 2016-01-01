/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

public interface NeoCommandVisitor
{
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException;
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException;
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException;
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException;
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws IOException;
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException;
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException;
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException;
    public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException;
}
