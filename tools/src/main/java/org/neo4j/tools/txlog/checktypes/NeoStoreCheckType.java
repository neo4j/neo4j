/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.record.NeoStoreRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

public class NeoStoreCheckType extends CheckType<Command.NeoStoreCommand,NeoStoreRecord>
{
    NeoStoreCheckType()
    {
        super( Command.NeoStoreCommand.class );
    }

    @Override
    public NeoStoreRecord before( Command.NeoStoreCommand command )
    {
        return command.getBefore();
    }

    @Override
    public NeoStoreRecord after( Command.NeoStoreCommand command )
    {
        return command.getAfter();
    }

    @Override
    public boolean equal( NeoStoreRecord record1, NeoStoreRecord record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        return record1.getNextProp() == record2.getNextProp();
    }

    @Override
    public String name()
    {
        return "neo_store";
    }
}
