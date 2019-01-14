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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.storageengine.api.StorageCommand;

public class PhysicalTransactionRepresentation implements TransactionRepresentation
{
    private final Collection<StorageCommand> commands;
    private byte[] additionalHeader;
    private int masterId;
    private int authorId;
    private long timeStarted;
    private long latestCommittedTxWhenStarted;
    private long timeCommitted;

    /**
     * This is a bit of a smell, it's used only for committing slave transactions on the master. Effectively, this
     * identifies the lock session used to guard this transaction. The master ensures that lock session is live before
     * committing, to guard against locks timing out. We may want to refactor this design later on.
     */
    private int lockSessionIdentifier;

    public PhysicalTransactionRepresentation( Collection<StorageCommand> commands )
    {
        this.commands = commands;
    }

    public void setHeader( byte[] additionalHeader, int masterId, int authorId, long timeStarted,
                           long latestCommittedTxWhenStarted, long timeCommitted, int lockSession )
    {
        this.additionalHeader = additionalHeader;
        this.masterId = masterId;
        this.authorId = authorId;
        this.timeStarted = timeStarted;
        this.latestCommittedTxWhenStarted = latestCommittedTxWhenStarted;
        this.timeCommitted = timeCommitted;
        this.lockSessionIdentifier = lockSession;
    }

    @Override
    public boolean accept( Visitor<StorageCommand,IOException> visitor ) throws IOException
    {
        for ( StorageCommand command : commands )
        {
            if ( visitor.visit( command ) )
            {
                return true;
            }
        }
        return false;
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
    public int getLockSessionId()
    {
        return lockSessionIdentifier;
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
        return authorId == that.authorId
               && latestCommittedTxWhenStarted == that.latestCommittedTxWhenStarted
               && masterId == that.masterId
               && timeStarted == that.timeStarted
               && Arrays.equals( additionalHeader, that.additionalHeader )
               && commands.equals( that.commands );
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

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( getClass().getSimpleName() ).append( '[' );
        builder.append( "masterId:" ).append( masterId ).append( ',' );
        builder.append( "authorId:" ).append( authorId ).append( ',' );
        builder.append( "timeStarted:" ).append( timeStarted ).append( ',' );
        builder.append( "latestCommittedTxWhenStarted:" ).append( latestCommittedTxWhenStarted ).append( ',' );
        builder.append( "timeCommitted:" ).append( timeCommitted ).append( ',' );
        builder.append( "lockSession:" ).append( lockSessionIdentifier ).append( ',' );
        builder.append( "additionalHeader:" ).append( Arrays.toString( additionalHeader ) );
        for ( StorageCommand command : commands )
        {
            builder.append( '\n' ).append( command );
        }
        return builder.toString();
    }
}
