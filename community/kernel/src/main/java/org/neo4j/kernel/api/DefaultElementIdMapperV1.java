/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.api;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

import org.neo4j.common.EntityType;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.values.ElementIdMapper;

import static java.lang.String.format;

/**
 * Produces element IDs which includes version, entity type, database ID and the internal storage entity ID.
 * The data is encoded into a {@code }byte[]} and converted into a String using base64.
 */
public class DefaultElementIdMapperV1 implements ElementIdMapper
{
    private static final byte ELEMENT_ID_FORMAT_VERSION = 1;

    private final StorageEngine storageEngine;
    private final UUID databaseId;

    public DefaultElementIdMapperV1( StorageEngine storageEngine, NamedDatabaseId databaseId )
    {
        this.storageEngine = storageEngine;
        this.databaseId = databaseId.databaseId().uuid();
    }

    @Override
    public String nodeElementId( long nodeId )
    {
        return buildElementId( EntityType.NODE, ELEMENT_ID_FORMAT_VERSION, storageEngine.encodeNodeId( nodeId ) );
    }

    @Override
    public long nodeId( String id )
    {
        ByteBuffer decoded = decodeElementId( id, EntityType.NODE );
        return storageEngine.decodeNodeId( decoded.array(), decoded.position() );
    }

    @Override
    public String relationshipElementId( long relationshipId )
    {
        return buildElementId( EntityType.RELATIONSHIP, ELEMENT_ID_FORMAT_VERSION, storageEngine.encodeRelationshipId( relationshipId ) );
    }

    @Override
    public long relationshipId( String id )
    {
        ByteBuffer decoded = decodeElementId( id, EntityType.RELATIONSHIP );
        return storageEngine.decodeRelationshipId( decoded.array(), 1 + Long.BYTES * 2 );
    }

    /**
     * Builds an element ID. This can probably be optimized, but the format is somewhat sensible in that it contains a header w/ format version,
     * database UUID and the storage-specific element ID.
     */
    private String buildElementId( EntityType entityType, byte elementIdFormatVersion, byte[] storageId )
    {
        ByteBuffer buffer = ByteBuffer.allocate( 1 + Long.BYTES * 2 + storageId.length );
        buffer.put( buildElementIdByteArrayHeader( entityType, elementIdFormatVersion ) );
        buffer.putLong( databaseId.getLeastSignificantBits() );
        buffer.putLong( databaseId.getMostSignificantBits() );
        buffer.put( storageId );
        return Base64.getEncoder().encodeToString( buffer.array() );
    }

    private ByteBuffer decodeElementId( String id, EntityType entityType )
    {
        byte[] decodedBytes = Base64.getDecoder().decode( id );
        ByteBuffer decoded = ByteBuffer.wrap( decodedBytes );
        verifyHeader( id, decoded, entityType );
        verifyDatabaseId( decoded );
        return decoded;
    }

    private void verifyHeader( String id, ByteBuffer decoded, EntityType entityType )
    {
        byte header = decoded.get();
        byte version = (byte) (header >>> 2);
        if ( version != ELEMENT_ID_FORMAT_VERSION )
        {
            throw new IllegalArgumentException( format( "Element ID %s has an unexpected version %d", id, version ) );
        }
        verifyEntityType( id, header, entityType );
    }

    private void verifyDatabaseId( ByteBuffer decoded )
    {
        long uuidLsb = decoded.getLong();
        long uuidMsb = decoded.getLong();
        if ( databaseId.getLeastSignificantBits() != uuidLsb || databaseId.getMostSignificantBits() != uuidMsb )
        {
            throw new IllegalArgumentException( "Element ID %s is for another database" );
        }
    }

    private void verifyEntityType( String id, byte header, EntityType expected )
    {
        byte entityTypeId = (byte) (header & 0x3);
        EntityType entityType = switch ( entityTypeId )
                {
                    case 0 -> EntityType.NODE;
                    case 1 -> EntityType.RELATIONSHIP;
                    default -> throw new IllegalArgumentException( format( "Element ID %s has unknown entity type ID %s", id, entityTypeId ) );
                };
        if ( entityType != expected )
        {
            throw new IllegalArgumentException( format( "Element ID %s has unexpected entity type %s, was expecting %s", id, entityType, expected ) );
        }
    }

    /**
     * A simple header in the element ID containing entity type and version of the element ID (should we ever change it).
     * The format of this version header must remain the same tho.
     */
    private byte buildElementIdByteArrayHeader( EntityType entityType, byte version )
    {
        byte header = 0;
        header |= entityType == EntityType.NODE ? 0 : 1;
        header |= version << 2;
        return header;
    }
}
