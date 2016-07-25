/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.txlog.checktypes;

import java.util.Objects;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

public class RelationshipGroupCheckType extends CheckType<Command.RelationshipGroupCommand,RelationshipGroupRecord>
{
    RelationshipGroupCheckType()
    {
        super( Command.RelationshipGroupCommand.class );
    }

    @Override
    public RelationshipGroupRecord before( Command.RelationshipGroupCommand command )
    {
        return command.getBefore();
    }

    @Override
    public RelationshipGroupRecord after( Command.RelationshipGroupCommand command )
    {
        return command.getAfter();
    }

    @Override
    public boolean equal( RelationshipGroupRecord record1, RelationshipGroupRecord record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        return record1.getId() == record2.getId() &&
               record1.inUse() == record2.inUse() &&
               record1.getFirstIn() == record2.getFirstIn() &&
               record1.getFirstLoop() == record2.getFirstLoop() &&
               record1.getFirstOut() == record2.getFirstOut() &&
               record1.getNext() == record2.getNext() &&
               record1.getOwningNode() == record2.getOwningNode() &&
               record1.getPrev() == record2.getPrev() &&
               record1.getType() == record2.getType();
    }

    @Override
    public String name()
    {
        return "relationship_group";
    }
}
