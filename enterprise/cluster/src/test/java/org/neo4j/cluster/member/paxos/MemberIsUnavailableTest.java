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
package org.neo4j.cluster.member.paxos;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class MemberIsUnavailableTest
{
    @Test
    public void shouldBeSerializedWhenClusterUriIsNull() throws IOException
    {
        // Given
        MemberIsUnavailable message = new MemberIsUnavailable( "master", new InstanceId( 1 ), null );

        // When
        byte[] serialized = serialize( message );

        // Then
        assertNotEquals( 0, serialized.length );
    }

    @Test
    public void shouldBeDeserializedWhenClusterUriIsNull() throws Exception
    {
        // Given
        MemberIsUnavailable message = new MemberIsUnavailable( "slave", new InstanceId( 1 ), null );
        byte[] serialized = serialize( message );

        // When
        MemberIsUnavailable deserialized = deserialize( serialized );

        // Then
        assertNotSame( message, deserialized );
        assertEquals( "slave", message.getRole() );
        assertEquals( new InstanceId( 1 ), message.getInstanceId() );
        assertNull( message.getClusterUri() );
    }

    private static byte[] serialize( MemberIsUnavailable message ) throws IOException
    {
        ObjectOutputStream outputStream = null;
        try
        {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            outputStream = new ObjectStreamFactory().create( byteArrayOutputStream );
            outputStream.writeObject( message );
            return byteArrayOutputStream.toByteArray();
        }
        finally
        {
            if ( outputStream != null )
            {
                outputStream.close();
            }
        }
    }

    private static MemberIsUnavailable deserialize( byte[] serialized ) throws Exception
    {
        ObjectInputStream inputStream = null;
        try
        {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream( serialized );
            inputStream = new ObjectStreamFactory().create( byteArrayInputStream );
            return (MemberIsUnavailable) inputStream.readObject();
        }
        finally
        {
            if ( inputStream != null )
            {
                inputStream.close();
            }
        }
    }
}
