/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.storageengine.api.StorageCommand;

public class MadeUpServerImplementation implements MadeUpCommunicationInterface
{
    private final StoreId storeIdToRespondWith;
    private boolean gotCalled;

    public MadeUpServerImplementation( StoreId storeIdToRespondWith )
    {
        this.storeIdToRespondWith = storeIdToRespondWith;
    }

    @Override
    public Response<Integer> multiply( int value1, int value2 )
    {
        gotCalled = true;
        return new TransactionStreamResponse<>( value1 * value2, storeIdToRespondWith,
                TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Void> fetchDataStream( MadeUpWriter writer, int dataSize )
    {
        // Reversed on the server side. This will send data back to the client.
        writer.write( new KnownDataByteChannel( dataSize ) );
        return emptyResponse();
    }

    private Response<Void> emptyResponse()
    {
        return new TransactionStreamResponse<>( null, storeIdToRespondWith,
                TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Void> sendDataStream( ReadableByteChannel data )
    {
        // TOOD Verify as well?
        readFully( data );
        return emptyResponse();
    }

    @Override
    public Response<Integer> streamBackTransactions( int responseToSendBack, final int txCount )
    {
        TransactionStream transactions = visitor ->
        {
            for ( int i = 1; i <= txCount; i++ )
            {
                CommittedTransactionRepresentation transaction =
                        createTransaction( TransactionIdStore.BASE_TX_ID + i );
                visitor.visit( transaction );
            }
        };

        return new TransactionStreamResponse<>( responseToSendBack, storeIdToRespondWith, transactions,
                ResourceReleaser.NO_OP );
    }

    @Override
    public Response<Integer> informAboutTransactionObligations( int responseToSendBack, long desiredObligation )
    {
        return new TransactionObligationResponse<>( responseToSendBack, storeIdToRespondWith, desiredObligation,
                ResourceReleaser.NO_OP );
    }

    protected CommittedTransactionRepresentation createTransaction( long txId )
    {
        return new CommittedTransactionRepresentation(
                new LogEntryStart( 0, 0, 0, 0, new byte[0], null ),
                transaction( txId ),
                new LogEntryCommit( txId, 0 ) );
    }

    private TransactionRepresentation transaction( long txId )
    {
        Collection<StorageCommand> commands = new ArrayList<>();
        NodeRecord node = new NodeRecord( txId );
        node.setInUse( true );
        commands.add( new NodeCommand( new NodeRecord( txId ), node ) );
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( commands );
        transaction.setHeader( new byte[0], 0, 0, 0, 0, 0, 0 );
        return transaction;
    }

    private void readFully( ReadableByteChannel data )
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1000 );
        try
        {
            while ( true )
            {
                buffer.clear();
                if ( data.read( buffer ) == -1 )
                {
                    break;
                }
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public Response<Integer> throwException( String messageInException )
    {
        throw new MadeUpException( messageInException, new Exception( "The cause of it" ) );
    }

    public boolean gotCalled()
    {
        return this.gotCalled;
    }
}
