/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

public class ReplicatedTransactionFactory
{

    private ReplicatedTransactionFactory()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static TransactionRepresentation extractTransactionRepresentation( ReplicatedTransaction transactionCommand, byte[] extraHeader )
    {
        return transactionCommand.extract( new TransactionRepresentationReader( extraHeader ) );
    }

    private static class TransactionRepresentationReader implements TransactionRepresentationExtractor
    {

        private final byte[] extraHeader;

        TransactionRepresentationReader( byte[] extraHeader )
        {
            this.extraHeader = extraHeader;
        }

        @Override
        public TransactionRepresentation extract( TransactionRepresentationReplicatedTransaction replicatedTransaction )
        {
            return replicatedTransaction.tx();
        }

        @Override
        public TransactionRepresentation extract( ByteArrayReplicatedTransaction replicatedTransaction )
        {
            ByteBuf buffer = Unpooled.wrappedBuffer( replicatedTransaction.getTxBytes() );
            NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( buffer );
            return read( channel );
        }

        @Override
        public TransactionRepresentation extract( ByteBufReplicatedTransaction replicatedTransaction )
        {
            NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( replicatedTransaction.content() );
            return read( channel );
        }

        private TransactionRepresentation read( NetworkReadableClosableChannelNetty4 channel )
        {
            try
            {
                LogEntryReader<ReadableClosablePositionAwareChannel> reader =
                        new VersionAwareLogEntryReader<>( new RecordStorageCommandReaderFactory(), InvalidLogEntryHandler.STRICT );

                int authorId = channel.getInt();
                int masterId = channel.getInt();
                long latestCommittedTxWhenStarted = channel.getLong();
                long timeStarted = channel.getLong();
                long timeCommitted = channel.getLong();
                int lockSessionId = channel.getInt();

                int headerLength = channel.getInt();
                byte[] header;
                if ( headerLength == 0 )
                {
                    header = extraHeader;
                }
                else
                {
                    header = new byte[headerLength];
                }

                channel.get( header, headerLength );

                LogEntryCommand entryRead;
                List<StorageCommand> commands = new LinkedList<>();

                while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
                {
                    commands.add( entryRead.getCommand() );
                }

                PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
                tx.setHeader( header, masterId, authorId, timeStarted, latestCommittedTxWhenStarted, timeCommitted, lockSessionId );

                return tx;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }
}
