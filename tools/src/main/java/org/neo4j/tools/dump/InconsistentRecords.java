/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.dump;

import java.util.EnumMap;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;

import static org.neo4j.collection.primitive.base.Empty.EMPTY_PRIMITIVE_LONG_SET;

/**
 * Container for ids of entities that are considered to be inconsistent.
 */
public class InconsistentRecords
{
    static long NO_ID = -1;

    enum Type
    {
        NODE,
        RELATIONSHIP,
        RELATIONSHIP_GROUP,
        PROPERTY,
        SCHEMA_INDEX,
        NODE_LABEL_RANGE
        {
            @Override
            public long extractId( String line )
            {
                // For the main report line there's nothing of interest... it's the next INCONSISTENT WITH line that is
                return NO_ID;
            }
        };

        public long extractId( String line )
        {
            int bracket = line.indexOf( '[' );
            if ( bracket > -1 )
            {
                int separator = min( getSeparatorIndex( ',', line, bracket ),
                        getSeparatorIndex( ';', line, bracket ),
                        getSeparatorIndex( ']', line, bracket ) );
                int equally = line.indexOf( '=', bracket );
                int startPosition = (isNotPlainId( bracket, separator, equally ) ? equally : bracket) + 1;
                if ( separator > -1 )
                {
                    return Long.parseLong( line.substring( startPosition, separator ) );
                }
            }
            return NO_ID;
        }

        private static int min( int... values )
        {
            int min = Integer.MAX_VALUE;
            for ( int value : values )
            {
                min = Math.min( min, value );
            }
            return min;
        }

        private int getSeparatorIndex( char character, String line, int bracket )
        {
            int index = line.indexOf( character, bracket );
            return index >= 0 ? index : Integer.MAX_VALUE;
        }

        private boolean isNotPlainId( int bracket, int comma, int equally )
        {
            return (equally > bracket) && (equally < comma);
        }
    }

    private final EnumMap<Type,PrimitiveLongSet> ids = new EnumMap<>( Type.class );

    public boolean containsId( Type recordType, long id )
    {
        return ids.getOrDefault( recordType, EMPTY_PRIMITIVE_LONG_SET ).contains( id );
    }

    public void reportInconsistency( Type recordType, long recordId )
    {
        if ( recordId != NO_ID )
        {
            ids.computeIfAbsent( recordType, t -> Primitive.longSet() ).add( recordId );
        }
    }
}
