/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.stresstest.workload;


import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.kernel.impl.api.TransactionHeaderInformation;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory.DEFAULT;

class TransactionRepresentationFactory
{
    private final CommandGenerator commandGenerator = new CommandGenerator();

    public TransactionRepresentation nextTransaction( long txId )
    {
        PhysicalTransactionRepresentation representation =
                new PhysicalTransactionRepresentation( createRandomCommands() );
        TransactionHeaderInformation headerInfo = DEFAULT.create();
        representation.setHeader( headerInfo.getAdditionalHeader(), headerInfo.getMasterId(),
                headerInfo.getAuthorId(), headerInfo.getAuthorId(), txId, currentTimeMillis(), 42 );
        return representation;
    }

    private Collection<Command> createRandomCommands()
    {
        int commandNum = ThreadLocalRandom.current().nextInt( 1, 17 );
        List<Command> commands = new ArrayList<>( commandNum );
        for ( int i = 0; i < commandNum; i++ )
        {
            commands.add( commandGenerator.nextCommand() );
        }
        return commands;
    }

    private static class CommandGenerator
    {
        private NodeRecordGenerator nodeRecordGenerator = new NodeRecordGenerator();

        public Command nextCommand()
        {
            Command.NodeCommand nodeCommand = new Command.NodeCommand();
            nodeCommand.init( nodeRecordGenerator.nextRecord(), nodeRecordGenerator.nextRecord() );
            return nodeCommand;
        }
    }

    private static class NodeRecordGenerator
    {

        public NodeRecord nextRecord()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            return new NodeRecord( random.nextLong(), random.nextBoolean(), random.nextBoolean(),
                    random.nextLong(), random.nextLong(), random.nextLong() );
        }
    }
}
