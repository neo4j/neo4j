/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_LABEL_CHANGES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_LABEL_FIELD;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_ADDITION;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_REMOVAL;

/**
 * Caches {@link InputNode} to disk using a binary format.
 */
public class InputNodeCacher extends InputEntityCacher<InputNode>
{
    private String[] previousLabels = InputEntity.NO_LABELS;

    public InputNodeCacher( StoreChannel channel, StoreChannel header, int bufferSize ) throws IOException
    {
        super( channel, header, bufferSize, 1 );
    }

    @Override
    protected void writeEntity( InputNode node ) throws IOException
    {
        // properties
        super.writeEntity( node );

        // group
        writeGroup( node.group(), 0 );

        // id
        writeValue( node.id() );

        // labels
        if ( node.hasLabelField() )
        {   // label field
            channel.put( HAS_LABEL_FIELD );
            channel.putLong( node.labelField() );
        }
        else
        {   // diff from previous node
            String[] labels = node.labels();
            writeDiff( LABEL_REMOVAL, previousLabels, labels );
            writeDiff( LABEL_ADDITION, labels, previousLabels );
            channel.put( END_OF_LABEL_CHANGES );
            previousLabels = labels;
        }
    }

    protected void writeDiff( byte mode, String[] compare, String[] with ) throws IOException
    {
        for ( String value : compare )
        {
            if ( !contains( with, value ) )
            {
                channel.put( mode );
                writeToken( value );
            }
        }
    }
}
