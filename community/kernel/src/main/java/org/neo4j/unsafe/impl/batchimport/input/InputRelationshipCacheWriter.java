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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_TYPE_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_TYPE;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_TYPE;

/**
 * Caches input relationships to disk using a binary format.
 */
public class InputRelationshipCacheWriter extends InputEntityCacheWriter
{
    public InputRelationshipCacheWriter( StoreChannel channel, StoreChannel header, RecordFormats recordFormats, int chunkSize )
    {
        super( channel, header, recordFormats, chunkSize );
    }

    @Override
    protected SerializingInputEntityVisitor instantiateWrapper( InputEntityVisitor visitor, int chunkSize )
    {
        return new SerializingInputRelationshipVisitor( visitor, chunkSize );
    }

    class SerializingInputRelationshipVisitor extends SerializingInputEntityVisitor
    {
        private String previousType;

        SerializingInputRelationshipVisitor( InputEntityVisitor actual, int chunkSize )
        {
            super( actual, chunkSize );
        }

        @Override
        protected void serializeEntity() throws IOException
        {
            // properties
            writeProperties();

            // groups
            writeGroup( startIdGroup, 0 );
            writeGroup( endIdGroup, 1 );

            // ids
            writeValue( startId() );
            writeValue( endId() );

            // type
            if ( hasIntType )
            {
                ByteBuffer buffer = buffer( Byte.BYTES + Integer.BYTES );
                buffer.put( HAS_TYPE_ID );
                buffer.putInt( intType );
            }
            else
            {
                if ( previousType != null && stringType.equals( previousType ) )
                {
                    buffer( Byte.BYTES ).put( SAME_TYPE );
                }
                else
                {
                    buffer( Byte.BYTES ).put( NEW_TYPE );
                    writeToken( RELATIONSHIP_TYPE_TOKEN, previousType = stringType );
                }
            }
        }

        @Override
        protected void clearState()
        {
            previousType = null;
            super.clearState();
        }
    }
}
