/**
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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.kernel.impl.nioneo.store.StoreId;

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
        return new Response<Integer>( value1 * value2, storeIdToRespondWith,
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
        return new Response<Void>( null, storeIdToRespondWith,
                TransactionStream.EMPTY, ResourceReleaser.NO_OP );
    }
    
    @Override
    public Response<Void> sendDataStream( ReadableByteChannel data )
    {
        // TOOD Verify as well?
        readFully( data );
        return emptyResponse();
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
                    break;
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
