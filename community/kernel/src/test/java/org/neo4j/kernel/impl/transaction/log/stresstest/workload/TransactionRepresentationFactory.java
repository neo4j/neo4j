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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory.DEFAULT;

class TransactionRepresentationFactory
{
    private final CommandGenerator commandGenerator = new CommandGenerator();

    TransactionToApply nextTransaction( long txId )
    {
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( createRandomCommands() );
        TransactionHeaderInformation headerInfo = DEFAULT.create();
        representation.setHeader( headerInfo.getAdditionalHeader(), headerInfo.getMasterId(),
                headerInfo.getAuthorId(), headerInfo.getAuthorId(), txId, currentTimeMillis(), 42 );
        return new TransactionToApply( representation );
    }

    private Collection<StorageCommand> createRandomCommands()
    {
        int commandNum = ThreadLocalRandom.current().nextInt( 1, 17 );
        List<StorageCommand> commands = new ArrayList<>( commandNum );
        for ( int i = 0; i < commandNum; i++ )
        {
            commands.add( commandGenerator.nextCommand() );
        }
        return commands;
    }

    private static class CommandGenerator
    {
        private NodeRecordGenerator nodeRecordGenerator = new NodeRecordGenerator();

        Command nextCommand()
        {
            return new Command.NodeCommand( nodeRecordGenerator.nextRecord(), nodeRecordGenerator.nextRecord() );
        }
    }

    private static class NodeRecordGenerator
    {

        NodeRecord nextRecord()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            return new NodeRecord( random.nextLong(), random.nextBoolean(), random.nextBoolean(),
                    random.nextLong(), random.nextLong(), random.nextLong() );
        }
    }
}
