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

import org.neo4j.csv.reader.SourceTraceability;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

/**
 * Builds {@link InputRelationship} from CSV data.
 */
public class InputRelationshipDeserialization extends InputEntityDeserialization<InputRelationship>
{
    private final Header header;
    private final Groups groups;

    private Group startNodeGroup;
    private Group endNodeGroup;
    private String type;
    private Object startNode;
    private Object endNode;

    public InputRelationshipDeserialization( SourceTraceability source, Header header, Groups groups )
    {
        super( source );
        this.header = header;
        this.groups = groups;
    }

    @Override
    public void initialize()
    {
        this.startNodeGroup = groups.getOrCreate( header.entry( Type.START_ID ).groupName() );
        this.endNodeGroup = groups.getOrCreate( header.entry( Type.END_ID ).groupName() );
    }

    @Override
    public void handle( Entry entry, Object value )
    {
        switch ( entry.type() )
        {
        case TYPE:
            type = (String) value;
            break;
        case START_ID:
            startNode = value;
            break;
        case END_ID:
            endNode = value;
            break;
        default:
            super.handle( entry, value );
            break;
        }
    }

    @Override
    public InputRelationship materialize()
    {
        return new InputRelationship(
                source.sourceDescription(), source.lineNumber(), source.position(),
                properties(), null, startNodeGroup, startNode, endNodeGroup, endNode, type, null );
    }

    @Override
    public void clear()
    {
        super.clear();
        type = null;
        startNode = endNode = null;
    }
}
