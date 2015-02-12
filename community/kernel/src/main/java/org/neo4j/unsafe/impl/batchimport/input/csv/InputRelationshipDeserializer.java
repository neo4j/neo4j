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

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.function.Function;
import org.neo4j.unsafe.impl.batchimport.input.DataException;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;

/**
 * {@link InputEntityDeserializer} that knows the semantics of an {@link InputRelationship} and how to extract that from
 * csv values using a {@link Header}.
 */
class InputRelationshipDeserializer extends InputEntityDeserializer<InputRelationship>
{
    // Additional data
    private String type;
    private Object startNode;
    private Object endNode;
    private final Group startNodeGroup;
    private final Group endNodeGroup;

    InputRelationshipDeserializer( Header header, CharSeeker data, int delimiter,
            Function<InputRelationship,InputRelationship> decorator, Groups groups )
    {
        super( header, data, delimiter, decorator );
        this.startNodeGroup = groups.getOrCreate( header.entry( Type.START_ID ).groupName() );
        this.endNodeGroup = groups.getOrCreate( header.entry( Type.END_ID ).groupName() );
    }

    @Override
    protected void handleValue( Header.Entry entry, Object value )
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
        }
    }

    @Override
    protected InputRelationship convertToInputEntity( Object[] properties )
    {
        return new InputRelationship( properties, null,
                startNodeGroup, startNode, endNodeGroup, endNode, type, null );
    }

    @Override
    protected void validate( InputRelationship entity )
    {
        if ( !entity.hasTypeId() && entity.type() == null )
        {
            throw new DataException( entity + " is missing " + Type.TYPE + " field" );
        }
    }
}
