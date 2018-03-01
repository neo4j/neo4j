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
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;

import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

class IndexDescriptorSerializer
{
    private static final byte LABEL_SCHEMA_DESCRIPTOR = 1;

    static void serialize( IndexDescriptor indexDescriptor, WritableChannel channel ) throws IOException
    {
        org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor schemaDescriptor = indexDescriptor.schema();
        if ( schemaDescriptor instanceof LabelSchemaDescriptor )
        {
            channel.put( LABEL_SCHEMA_DESCRIPTOR );
            LabelSchemaDescriptor labelSchemaDescriptor = (LabelSchemaDescriptor) schemaDescriptor;
            channel.putInt( labelSchemaDescriptor.getLabelId() );
            channel.putInt( labelSchemaDescriptor.getPropertyIds().length );
            for ( int propertyId : labelSchemaDescriptor.getPropertyIds() )
            {
                channel.putInt( propertyId );
            }
            channel.putInt( indexDescriptor.type().ordinal() );
        }
        else
        {
            throw new IllegalArgumentException( "Not a recognized LabelSchemaDescriptor class: " + indexDescriptor.schema().getClass() );
        }
    }

    static IndexDescriptor deserialize( ReadableChannel channel ) throws IOException
    {
        byte schemaDescriptor = channel.get();

        switch ( schemaDescriptor )
        {
        case LABEL_SCHEMA_DESCRIPTOR:
            int labelId = channel.getInt();
            int length = channel.getInt();
            int[] propertyIds = new int[length];
            for ( int i = 0; i < length; i++ )
            {
                propertyIds[i] = channel.getInt();
            }
            IndexDescriptor.Type type = IndexDescriptor.Type.values()[channel.getInt()];
            return new IndexDescriptor( new LabelSchemaDescriptor( labelId, propertyIds ), type );
        default:
            throw new IllegalStateException( "Unhandled schema descriptor: " + schemaDescriptor );
        }
    }
}
