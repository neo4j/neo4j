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

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;

import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_LABEL_CHANGES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_LABEL_FIELD;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_ADDITION;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_REMOVAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_TOKEN;

/**
 * Caches input nodes to disk using a binary format.
 */
public class InputNodeCacheWriter extends InputEntityCacheWriter
{
    public InputNodeCacheWriter( StoreChannel channel, StoreChannel header, RecordFormats recordFormats, int chunkSize )
    {
        super( channel, header, recordFormats, chunkSize );
    }

    @Override
    protected SerializingInputEntityVisitor instantiateWrapper( InputEntityVisitor visitor, int chunkSize )
    {
        return new SerializingInputNodeVisitor( visitor, chunkSize );
    }

    class SerializingInputNodeVisitor extends SerializingInputEntityVisitor
    {
        private String[] previousLabels = EMPTY_STRING_ARRAY;

        SerializingInputNodeVisitor( InputEntityVisitor actual, int chunkSize )
        {
            super( actual, chunkSize );
        }

        @Override
        protected void serializeEntity() throws IOException
        {
            // properties
            writeProperties();

            // group
            writeGroup( idGroup, 0 );

            // id
            writeValue( id() );

            // labels
            if ( hasLabelField )
            {   // label field
                buffer( Byte.BYTES + Long.BYTES ).put( HAS_LABEL_FIELD ).putLong( labelField );
            }
            else
            {   // diff from previous node
                String[] labels = labels();
                writeLabelDiff( LABEL_REMOVAL, previousLabels, labels );
                writeLabelDiff( LABEL_ADDITION, labels, previousLabels );
                buffer( Byte.BYTES ).put( END_OF_LABEL_CHANGES );
                previousLabels = labels;
            }
        }

        @Override
        protected void clearState()
        {
            previousLabels = EMPTY_STRING_ARRAY;
            super.clearState();
        }

        protected void writeLabelDiff( byte mode, String[] compare, String[] with ) throws IOException
        {
            for ( String value : compare )
            {
                if ( !contains( with, value ) )
                {
                    buffer( Byte.BYTES ).put( mode );
                    writeToken( LABEL_TOKEN, value );
                }
            }
        }
    }
}
