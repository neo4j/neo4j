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

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_TYPE;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_TYPE;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SPECIFIC_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_TYPE_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

/**
 * Reads cached {@link InputRelationship} previously stored using {@link InputRelationshipCacher}.
 */
public class InputRelationshipReader extends InputEntityReader<InputRelationship>
{
    private String previousType;

    public InputRelationshipReader( StoreChannel channel, StoreChannel header, int bufferSize ) throws IOException
    {
        super( channel, header, bufferSize, 2 );
    }

    @Override
    protected InputRelationship readNextOrNull( Object properties ) throws IOException
    {
        // id
        long specificId = channel.get() == SPECIFIC_ID ? channel.getLong() : -1;

        // groups
        Group startNodeGroup = readGroup( 0 );
        Group endNodeGroup = readGroup( 1 );

        // ids
        Object startNodeId = readValue();
        Object endNodeId = readValue();

        // type
        byte typeMode = channel.get();
        Object type;
        switch ( typeMode )
        {
        case SAME_TYPE: type = previousType; break;
        case NEW_TYPE: type = previousType = readToken(); break;
        case HAS_TYPE_ID: type = channel.getInt(); break;
        default: throw new IllegalArgumentException( "Unrecognized type mode " + typeMode );
        }

        InputRelationship relationship = new InputRelationship( sourceDescription(), lineNumber(), position(),
                properties.getClass().isArray() ? (Object[]) properties : NO_PROPERTIES,
                properties.getClass().isArray() ? null : (Long) properties,
                startNodeGroup, startNodeId,
                endNodeGroup, endNodeId,
                type instanceof String ? (String) type : null,
                type instanceof String ? null : (Integer) type );
        return specificId != -1 ? relationship.setSpecificId( specificId ) : relationship;
    }
}
