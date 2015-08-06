/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.messaging.v1;

import java.io.IOException;

import static org.neo4j.packstream.PackStream.UTF_8;

public class IdentityPack
{
    public static final byte[] NODE_IDENTITY_PREFIX = "node/".getBytes( UTF_8 );
    public static final byte[] REL_IDENTITY_PREFIX = "rel/".getBytes( UTF_8 );

    public static class Packer
    {

        public void packNodeIdentity( Neo4jPack.Packer packer, long id ) throws IOException
        {
            packer.packText( NODE_IDENTITY_PREFIX, id );
        }

        public void packRelationshipIdentity( Neo4jPack.Packer packer, long id ) throws IOException
        {
            packer.packText( REL_IDENTITY_PREFIX, id );
        }

    }

    public static class Unpacker
    {
        private static final int MAX_IDENTITY_SIZE = 30;  // should be plenty

        private final byte[] buffer = new byte[MAX_IDENTITY_SIZE];

        private void assertHasPrefix( byte[] prefix )
        {
            for ( int i = 0; i < prefix.length; i++ )
            {
                if ( buffer[i] != prefix[i] )
                {
                    throw new AssertionError( "Expected identity prefix not found" );
                }
            }
        }

        private long unpackLongFromText( int startOffset, int endOffset )
        {
            long id = 0;
            for ( int i = startOffset; i < endOffset; i++ )
            {
                int digit = buffer[i] - 48;
                id = 10 * id + digit;
            }
            return id;
        }

        public long unpackNodeIdentity( Neo4jPack.Unpacker unpacker ) throws IOException
        {
            int size = unpacker.unpackUTF8Into( buffer, 0, MAX_IDENTITY_SIZE );
            assertHasPrefix( NODE_IDENTITY_PREFIX );
            return unpackLongFromText( NODE_IDENTITY_PREFIX.length, size );
        }

        public long unpackRelationshipIdentity( Neo4jPack.Unpacker unpacker ) throws IOException
        {
            int size = unpacker.unpackUTF8Into( buffer, 0, MAX_IDENTITY_SIZE );
            assertHasPrefix( REL_IDENTITY_PREFIX );
            return unpackLongFromText( REL_IDENTITY_PREFIX.length, size );
        }

    }

}
