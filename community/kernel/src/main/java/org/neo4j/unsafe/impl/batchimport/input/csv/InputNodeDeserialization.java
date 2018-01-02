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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import java.util.Arrays;

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static java.util.Arrays.copyOf;

/**
 * Builds {@link InputNode} from CSV data.
 */
public class InputNodeDeserialization extends InputEntityDeserialization<InputNode>
{
    private final Header header;
    private final Groups groups;

    private final boolean idsAreExternal;
    private Group group;
    private Object id;
    private String[] labels = new String[10];
    private int labelsCursor;

    public InputNodeDeserialization( SourceTraceability source, Header header, Groups groups, boolean idsAreExternal )
    {
        super( source );
        this.header = header;
        this.groups = groups;
        this.idsAreExternal = idsAreExternal;
    }

    @Override
    public void initialize()
    {
        // ID header entry is optional
        Entry idEntry = header.entry( Type.ID );
        this.group = groups.getOrCreate( idEntry != null ? idEntry.groupName() : null );
    }

    @Override
    public void handle( Entry entry, Object value )
    {
        switch ( entry.type() )
        {
        case ID:
            if ( entry.name() != null && idsAreExternal )
            {
                addProperty( entry.name(), value );
            }
            id = value;
            break;
        case LABEL:
            addLabels( value );
            break;
        default:
            super.handle( entry, value );
            break;
        }
    }

    @Override
    public InputNode materialize()
    {
        return new InputNode(
                source.sourceDescription(), source.lineNumber(), source.position(),
                group, id, properties(), null, labels(), null );
    }

    @Override
    public void clear()
    {
        super.clear();
        labelsCursor = 0;
        id = null;
    }

    private void ensureLabelsCapacity( int length )
    {
        if ( length > labels.length )
        {
            labels = Arrays.copyOf( labels, length );
        }
    }

    private void addLabels( Object value )
    {
        if ( value instanceof String )
        {
            ensureLabelsCapacity( labelsCursor + 1 );
            labels[labelsCursor++] = (String) value;
        }
        else if ( value instanceof String[] )
        {
            String[] labelsToAdd = (String[]) value;
            ensureLabelsCapacity( labelsCursor + labelsToAdd.length );
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
    }

    private String[] labels()
    {
        return labelsCursor > 0
                ? copyOf( labels, labelsCursor )
                : InputEntity.NO_LABELS;
    }
}
