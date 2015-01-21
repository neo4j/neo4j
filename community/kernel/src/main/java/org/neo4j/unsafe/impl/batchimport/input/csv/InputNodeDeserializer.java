/**
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.util.Arrays;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.util.Arrays.copyOf;

/**
 * {@link InputEntityDeserializer} that knows the semantics of an {@link InputNode} and how to extract that from
 * csv values using a {@link Header}.
 */
class InputNodeDeserializer extends InputEntityDeserializer<InputNode>
{
    private final boolean idsAreExternal;

    // Additional data
    private Object id;
    // holder of labels, Will grow with the node having most labels.
    private String[] labels = new String[10];
    private int labelsCursor;
    private final Group group;

    InputNodeDeserializer( Header header, CharSeeker data, int[] delimiter, Function<InputNode,InputNode> decorator,
            boolean idsAreExternal, Groups groups )
    {
        super( header, data, delimiter, decorator );
        this.idsAreExternal = idsAreExternal;

        // ID header entry is optional
        Entry idEntry = header.entry( Type.ID );
        this.group = idEntry != null ? groups.getOrCreate( idEntry.groupName() ) : Group.GLOBAL;
    }

    @Override
    protected void handleValue( Header.Entry entry, Object value )
    {
        switch ( entry.type() )
        {
        case ID:
            if ( entry.name() != null && idsAreExternal )
            {
                addProperty( entry, value );
            }
            id = value;
            break;
        case LABEL:
            if ( value instanceof String )
            {
                ensureLabelsCapacity( labelsCursor+1 );
                labels[labelsCursor++] = (String) value;
            }
            else if ( value instanceof String[] )
            {
                String[] labelsToAdd = (String[]) value;
                ensureLabelsCapacity( labelsCursor+labelsToAdd.length );
                for ( String label : (String[]) value )
                {
                    labels[labelsCursor++] = label;
                }
            }
            else
            {
                throw new IllegalArgumentException( "Unexpected label value type " +
                        value.getClass() + ": " + value );
            }
            break;
        }
    }

    private void ensureLabelsCapacity( int length )
    {
        if ( length > labels.length )
        {
            labels = Arrays.copyOf( labels, length );
        }
    }

    @Override
    protected InputNode convertToInputEntity( Object[] properties )
    {
        try
        {
            return new InputNode( group, id, properties, null, labels(), null );
        }
        finally
        {
            id = null;
            labelsCursor = 0;
        }
    }

    private String[] labels()
    {
        return labelsCursor > 0
                ? copyOf( labels, labelsCursor )
                : InputEntity.NO_LABELS;
    }
}
