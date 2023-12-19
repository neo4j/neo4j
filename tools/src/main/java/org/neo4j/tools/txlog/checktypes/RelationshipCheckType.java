/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.tools.txlog.checktypes;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

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

    @Override
    protected boolean inUseRecordsEqual( RelationshipRecord record1, RelationshipRecord record2 )
    {
        return record1.getNextProp() == record2.getNextProp() &&
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
