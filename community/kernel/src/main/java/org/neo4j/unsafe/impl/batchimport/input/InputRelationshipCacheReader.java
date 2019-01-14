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

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_TYPE_ID;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.NEW_TYPE;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.RELATIONSHIP_TYPE_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.SAME_TYPE;

/**
 * Reads cached input relationships previously stored using {@link InputRelationshipCacheWriter}.
 */
public class InputRelationshipCacheReader extends InputEntityCacheReader
{
    public InputRelationshipCacheReader( StoreChannel channel, StoreChannel header ) throws IOException
    {
        super( channel, header );
    }

    @Override
    public InputChunk newChunk()
    {
        return new InputRelationshipDeserializer();
    }

    class InputRelationshipDeserializer extends InputEntityDeserializer
    {
        protected String previousType;

        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            if ( !readProperties( visitor ) )
            {
                return false;
            }

            // groups
            Group startNodeGroup = readGroup( 0 );
            Group endNodeGroup = readGroup( 1 );

            // ids
            Object startNodeId = readValue();
            Object endNodeId = readValue();
            visitor.startId( startNodeId, startNodeGroup );
            visitor.endId( endNodeId, endNodeGroup );

            // type
            byte typeMode = channel.get();
            switch ( typeMode )
            {
            case SAME_TYPE:
                visitor.type( previousType );
                break;
            case NEW_TYPE:
                visitor.type( previousType = (String) readToken( RELATIONSHIP_TYPE_TOKEN ) );
                break;
            case HAS_TYPE_ID:
                visitor.type( channel.getInt() );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized type mode " + typeMode );
            }
            return true;
        }
    }
}
