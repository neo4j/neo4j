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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.PhysicalLogCommandReaderV3_0_2;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RelationshipCheckType extends CheckType<Command.RelationshipCommand,RelationshipRecord>
{
    RelationshipCheckType()
    {
        super( Command.RelationshipCommand.class );
    }

    @Override
    public RelationshipRecord before( Command.RelationshipCommand command )
    {
        return command.getBefore();
    }

    @Override
    public RelationshipRecord after( Command.RelationshipCommand command )
    {
        return command.getAfter();
    }

    /**
     * Checks that two given {@link RelationshipRecord}s are equal. {@link RelationshipRecord#inUse() Used}
     * records are compared by all present fields/pointers. Unused records are compared only by id. This is so
     * because for removed relationships we write not only inUse flag but also
     * {@link RelationshipRecord#getType() relationship type} in
     * {@link Command.RelationshipCommand#serialize(WritableChannel)} and read it in
     * {@link PhysicalLogCommandReaderV3_0_2#readRelationshipRecord(long, ReadableChannel)}.
     *
     * @param record1 first record to check.
     * @param record2 second record to check.
     * @return {@code true} when records are equal, otherwise {@code false}.
     */
    @Override
    public boolean equal( RelationshipRecord record1, RelationshipRecord record2 )
    {
        Objects.requireNonNull( record1 );
        Objects.requireNonNull( record2 );

        if ( record1.getId() != record2.getId() )
        {
            return false;
        }

        if ( record1.inUse() == record2.inUse() && !record1.inUse() )
        {
            return true;
        }

        return record1.inUse() == record2.inUse() &&
               record1.getNextProp() == record2.getNextProp() &&
               record1.isFirstInFirstChain() == record2.isFirstInFirstChain() &&
               record1.isFirstInSecondChain() == record2.isFirstInSecondChain() &&
               record1.getFirstNextRel() == record2.getFirstNextRel() &&
               record1.getFirstNode() == record2.getFirstNode() &&
               record1.getFirstPrevRel() == record2.getFirstPrevRel() &&
               record1.getSecondNextRel() == record2.getSecondNextRel() &&
               record1.getSecondNode() == record2.getSecondNode() &&
               record1.getSecondPrevRel() == record2.getSecondPrevRel() &&
               record1.getType() == record2.getType();
    }


    @Override
    public String name()
    {
        return "relationship";
    }
}
