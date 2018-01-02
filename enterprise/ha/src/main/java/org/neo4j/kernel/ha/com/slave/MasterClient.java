/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.slave;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.ha.MasterClient214;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.ha.lock.LockStatus;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.util.HexPrinter;

import static java.lang.String.format;

import static org.neo4j.com.Protocol.readString;
import static org.neo4j.com.Protocol.writeString;

public interface MasterClient extends Master
{
    static final ObjectSerializer<LockResult> LOCK_SERIALIZER = new ObjectSerializer<LockResult>()
    {
        @Override
        public void write( LockResult responseObject, ChannelBuffer result ) throws IOException
        {
            result.writeByte( responseObject.getStatus().ordinal() );
            if ( responseObject.getStatus().hasMessage() )
            {
                writeString( result, responseObject.getDeadlockMessage() );
            }
        }
    };

    static final Deserializer<LockResult> LOCK_RESULT_DESERIALIZER = new Deserializer<LockResult>()
    {
        @Override
        public LockResult read( ChannelBuffer buffer, ByteBuffer temporaryBuffer ) throws IOException
        {
            byte statusOrdinal = buffer.readByte();
            LockStatus status;
            try
            {
                status = LockStatus.values()[statusOrdinal];
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {
                int maxBytesToPrint = 1024*40;
                throw Exceptions.withMessage( e, format( "%s | read invalid ordinal %d. First %db of this channel buffer is:%n%s",
                        e.getMessage(), statusOrdinal, maxBytesToPrint, beginningOfBufferAsHexString( buffer, maxBytesToPrint ) ) );
            }
            return status.hasMessage() ? new LockResult( readString( buffer ) ) : new LockResult( status );
        }

        private String beginningOfBufferAsHexString( ChannelBuffer buffer, int maxBytesToPrint )
        {
            // read buffer from pos 0 - writeIndex
            int prevIndex = buffer.readerIndex();
            buffer.readerIndex( 0 );
            try
            {
                ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream( buffer.readableBytes() );
                PrintStream stream = new PrintStream( byteArrayStream );
                HexPrinter printer = new HexPrinter( stream ).withLineNumberDigits( 4 );
                for ( int i = 0; buffer.readable() && i < maxBytesToPrint; i++ )
                {
                    printer.append( buffer.readByte() );
                }
                stream.flush();
                return byteArrayStream.toString();
            }
            finally
            {
                buffer.readerIndex( prevIndex );
            }
        }
    };

    public static final ProtocolVersion CURRENT = MasterClient214.PROTOCOL_VERSION;

    @Override
    public Response<Integer> createRelationshipType( RequestContext context, final String name );

    @Override
    public Response<Void> newLockSession( RequestContext context );

    @Override
    public Response<Long> commit( RequestContext context, final TransactionRepresentation channel );

    @Override
    public Response<Void> pullUpdates( RequestContext context );

    public Response<Void> pullUpdates( RequestContext context, TxHandler txHandler );

    @Override
    public Response<Void> copyStore( RequestContext context, final StoreWriter writer );

    public void setComExceptionHandler( ComExceptionHandler handler );

    public ProtocolVersion getProtocolVersion();
}
