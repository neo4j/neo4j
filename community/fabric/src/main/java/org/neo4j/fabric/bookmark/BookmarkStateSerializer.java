/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.bookmark;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import org.neo4j.bolt.packstream.PackOutput;
import org.neo4j.bolt.packstream.PackStream;
import org.neo4j.bolt.packstream.PackedInputArray;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.executor.FabricException;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.InvalidBookmark;

public class BookmarkStateSerializer
{
    public static String serialize( FabricBookmark fabricBookmark )
    {
        try
        {
            var packer = new Packer( fabricBookmark );
            var p = packer.pack();
            return p;
        }
        catch ( IOException exception )
        {
            // if this fails, it means a bug
            throw new IllegalStateException( "Failed to serialize bookmark", exception );
        }
    }

    public static FabricBookmark deserialize( String serializedBookmark )
    {
        try
        {
            var unpacker = new Unpacker( serializedBookmark );
            return unpacker.unpack();
        }
        catch ( IOException exception )
        {
            throw new FabricException( InvalidBookmark, "Failed to deserialize bookmark", exception );
        }
    }

    private static class Packer
    {
        final PackedOutputArray packedOutputArray = new PackedOutputArray();
        final PackStream.Packer packer = new BookmarkPackStreamPacker( packedOutputArray );
        final FabricBookmark fabricBookmark;

        Packer( FabricBookmark fabricBookmark )
        {
            this.fabricBookmark = fabricBookmark;
        }

        String pack() throws IOException
        {
            packInternalGraphs( fabricBookmark.getInternalGraphStates() );
            packExternalGraphs( fabricBookmark.getExternalGraphStates() );
            packer.flush();
            return Base64.getEncoder().encodeToString( packedOutputArray.bytes() );
        }

        void packInternalGraphs( List<FabricBookmark.InternalGraphState> internalGraphStates ) throws IOException
        {
            packer.packListHeader( internalGraphStates.size() );

            for ( var internalGraphState : internalGraphStates )
            {
                packInternalGraph( internalGraphState );
            }
        }

        void packInternalGraph( FabricBookmark.InternalGraphState internalGraphState ) throws IOException
        {
            packUuid( internalGraphState.getGraphUuid() );
            packer.pack( internalGraphState.getTransactionId() );
        }

        void packExternalGraphs( List<FabricBookmark.ExternalGraphState> externalGraphStates ) throws IOException
        {
            packer.packListHeader( externalGraphStates.size() );

            for ( var externalGraphState : externalGraphStates )
            {
                packExternalGraph( externalGraphState );
            }
        }

        void packExternalGraph( FabricBookmark.ExternalGraphState externalGraphState ) throws IOException
        {
            packUuid( externalGraphState.getGraphUuid() );
            packer.packListHeader( externalGraphState.getBookmarks().size() );
            for ( var remoteBookmark : externalGraphState.getBookmarks() )
            {
                packer.pack( remoteBookmark.getSerialisedState() );
            }
        }

        void packUuid( UUID uuid ) throws IOException
        {
            ByteBuffer buffer = ByteBuffer.allocate( 16 );
            buffer.putLong( uuid.getMostSignificantBits() );
            buffer.putLong( uuid.getLeastSignificantBits() );
            packer.pack( buffer.array() );
        }
    }

    private static class Unpacker
    {
        final PackStream.Unpacker unpacker;

        Unpacker( String serializedBookmark )
        {
            var bytes = Base64.getDecoder().decode( serializedBookmark );
            var packedInputArray = new PackedInputArray( bytes );
            unpacker = new PackStream.Unpacker( packedInputArray );
        }

        FabricBookmark unpack() throws IOException
        {
            var internalGraphs = unpackInternalGraphs();
            var externalGraphs = unpackExternalGraphs();

            return new FabricBookmark( internalGraphs, externalGraphs );
        }

        List<FabricBookmark.InternalGraphState> unpackInternalGraphs() throws IOException
        {
            int listSize = (int) unpacker.unpackListHeader();
            List<FabricBookmark.InternalGraphState> internalGraphs = new ArrayList<>( listSize );
            for ( int i = 0; i < listSize; i++ )
            {
                var internalGraphState = unpackInternalGraph();
                internalGraphs.add( internalGraphState );
            }

            return internalGraphs;
        }

        FabricBookmark.InternalGraphState unpackInternalGraph() throws IOException
        {
            UUID graphUuid = unpackUuid();
            long txId = unpacker.unpackLong();

            return new FabricBookmark.InternalGraphState( graphUuid, txId );
        }

        List<FabricBookmark.ExternalGraphState> unpackExternalGraphs() throws IOException
        {
            int listSize = (int) unpacker.unpackListHeader();
            List<FabricBookmark.ExternalGraphState> externalGraphs = new ArrayList<>( listSize );
            for ( int i = 0; i < listSize; i++ )
            {
                var externalGraphState = unpackExternalGraph();
                externalGraphs.add( externalGraphState );
            }

            return externalGraphs;
        }

        FabricBookmark.ExternalGraphState unpackExternalGraph() throws IOException
        {
            UUID graphUuid = unpackUuid();
            int listSize = (int) unpacker.unpackListHeader();

            List<RemoteBookmark> remoteBookmarks = new ArrayList<>( listSize );
            for ( int i = 0; i < listSize; i++ )
            {
                String serializedRemoteBookmark = unpacker.unpackString();
                remoteBookmarks.add( new RemoteBookmark( serializedRemoteBookmark ) );
            }

            return new FabricBookmark.ExternalGraphState( graphUuid, remoteBookmarks );
        }

        UUID unpackUuid() throws IOException
        {
            byte[] uuidBytes = unpacker.unpackBytes();
            ByteBuffer byteBuffer = ByteBuffer.wrap( uuidBytes );
            long high = byteBuffer.getLong();
            long low = byteBuffer.getLong();

            return new UUID( high, low );
        }
    }

    private static class BookmarkPackStreamPacker extends PackStream.Packer
    {
        // The default PackStream.Packer creates a UTF-8 encoder
        // whose construction is quite expensive (created using reflection, allocating 16K buffer, ...).
        // And since external bookmarks are the only Strings encoded,
        // it is better to use the JDK UTF 8 encoder.
        // Also the external bookmarks should be already UTF-8 encoded (we should not rely on that),
        // which means quite an easy job for the encoder.
        BookmarkPackStreamPacker( PackOutput out )
        {
            super( out, input -> ByteBuffer.wrap( input.getBytes( StandardCharsets.UTF_8 ) ) );
        }
    }
}
