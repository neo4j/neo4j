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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.Collection;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;

public class PhysicalTransactionRepresentation implements TransactionRepresentation
{
    private final Collection<Command> commands;
    private final boolean recovered;
    private byte[] additionalHeader;
    private int masterId;
    private int authorId;
    private long timeWritten;
    private long latestCommittedTxWhenStarted;

    // TODO 2.2-future recovered could be an aspect instead
    public PhysicalTransactionRepresentation( Collection<Command> commands, boolean recovered )
    {
        this.commands = commands;
        this.recovered = recovered;
    }

    public void setHeader( byte[] additionalHeader, int masterId, int authorId, long timeWritten,
            long latestCommittedTxWhenStarted )
    {
        this.additionalHeader = additionalHeader;
        this.masterId = masterId;
        this.authorId = authorId;
        this.timeWritten = timeWritten;
        this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
    }

    @Override
    public boolean isRecovered()
    {
        return recovered;
    }

    @Override
    public void accept( Visitor<Command, IOException> visitor ) throws IOException
    {
        for ( Command command : commands )
        {
            if (!visitor.visit( command ))
                return;
        }
    }

    @Override
    public byte[] additionalHeader()
    {
        return additionalHeader;
    }

    @Override
    public int getMasterId()
    {
        return masterId;
    }

    @Override
    public int getAuthorId()
    {
        return authorId;
    }

    @Override
    public long getTimeWritten()
    {
        return timeWritten;
    }

    @Override
    public long getLatestCommittedTxWhenStarted()
    {
        return latestCommittedTxWhenStarted;
    }
}
