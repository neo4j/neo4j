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
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;

public class PhysicalTransactionRepresentation implements TransactionRepresentation
{
    private final Collection<Command> commands;
    private byte[] additionalHeader;
    private int masterId;
    private int authorId;
    private long timeStarted;
    private long latestCommittedTxWhenStarted;
    private long timeCommitted;

    // TODO 2.2-future recovered could be an aspect instead
    public PhysicalTransactionRepresentation( Collection<Command> commands )
    {
        this.commands = commands;
    }

    public void setHeader( byte[] additionalHeader, int masterId, int authorId, long timeStarted,
            long latestCommittedTxWhenStarted, long timeCommitted )
    {
        this.additionalHeader = additionalHeader;
        this.masterId = masterId;
        this.authorId = authorId;
        this.timeStarted = timeStarted;
        this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        this.timeCommitted = timeCommitted;
    }

    @Override
    public void accept( Visitor<Command, IOException> visitor ) throws IOException
    {
        for ( Command command : commands )
        {
            if (!visitor.visit( command ))
            {
                return;
            }
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
    public long getTimeStarted()
    {
        return timeStarted;
    }

    @Override
    public long getLatestCommittedTxWhenStarted()
    {
        return latestCommittedTxWhenStarted;
    }

    @Override
    public long getTimeCommitted()
    {
        return timeCommitted;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        PhysicalTransactionRepresentation that = (PhysicalTransactionRepresentation) o;

        if ( authorId != that.authorId )
        {
            return false;
        }
        if ( latestCommittedTxWhenStarted != that.latestCommittedTxWhenStarted )
        {
            return false;
        }
        if ( masterId != that.masterId )
        {
            return false;
        }
        if ( timeStarted != that.timeStarted )
        {
            return false;
        }
        if ( !Arrays.equals( additionalHeader, that.additionalHeader ) )
        {
            return false;
        }
        if ( !commands.equals( that.commands ) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = commands.hashCode();
        result = 31 * result + (additionalHeader != null ? Arrays.hashCode( additionalHeader ) : 0);
        result = 31 * result + masterId;
        result = 31 * result + authorId;
        result = 31 * result + (int) (timeStarted ^ (timeStarted >>> 32));
        result = 31 * result + (int) (latestCommittedTxWhenStarted ^ (latestCommittedTxWhenStarted >>> 32));
        return result;
    }
}
