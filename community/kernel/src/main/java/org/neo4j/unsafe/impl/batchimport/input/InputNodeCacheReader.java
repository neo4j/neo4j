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
import java.util.Arrays;

import org.neo4j.io.fs.StoreChannel;

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_LABEL_CHANGES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_LABEL_FIELD;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_ADDITION;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_REMOVAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_TOKEN;

/**
 * Reads cached input nodes previously stored using {@link InputNodeCacheWriter}.
 */
public class InputNodeCacheReader extends InputEntityCacheReader
{
    public InputNodeCacheReader( StoreChannel channel, StoreChannel header ) throws IOException
    {
        super( channel, header );
    }

    @Override
    public InputChunk newChunk()
    {
        return new InputNodeDeserializer();
    }

    class InputNodeDeserializer extends InputEntityDeserializer
    {
        protected String[] previousLabels = InputEntityCacheWriter.EMPTY_STRING_ARRAY;

        @Override
        public boolean next( InputEntityVisitor visitor ) throws IOException
        {
            if ( !readProperties( visitor ) )
            {
                return false;
            }

            // group
            Group group = readGroup( 0 );

            // id
            Object id = readValue();
            visitor.id( id, group );

            // labels (diff from previous node)
            byte labelsMode = channel.get();
            if ( labelsMode == HAS_LABEL_FIELD )
            {
                visitor.labelField( channel.getLong() );
            }
            else if ( labelsMode == END_OF_LABEL_CHANGES )
            {   // Same as for previous node
                visitor.labels( previousLabels );
            }
            else
            {
                String[] newLabels = previousLabels.clone();
                int cursor = newLabels.length;
                while ( labelsMode != END_OF_LABEL_CHANGES )
                {
                    switch ( labelsMode )
                    {
                    case LABEL_REMOVAL:
                        remove( (String) readToken( LABEL_TOKEN ), newLabels, cursor-- );
                        break;
                    case LABEL_ADDITION:
                        (newLabels = ensureRoomForOneMore( newLabels, cursor ))[cursor++] = (String) readToken( LABEL_TOKEN );
                        break;
                    default:
                        throw new IllegalArgumentException( "Unrecognized label mode " + labelsMode );
                    }
                    labelsMode = channel.get();
                }
                visitor.labels( previousLabels = cursor == newLabels.length ? newLabels : Arrays.copyOf( newLabels, cursor ) );
            }
            return true;
        }

        private String[] ensureRoomForOneMore( String[] labels, int cursor )
        {
            return cursor >= labels.length ? Arrays.copyOf( labels, cursor + 1 ) : labels;
        }

        private void remove( String item, String[] from, int cursor )
        {
            for ( int i = 0; i < cursor; i++ )
            {
                if ( item.equals( from[i] ) )
                {
                    from[i] = from[cursor - 1];
                    from[cursor - 1] = null;
                    return;
                }
            }
            throw new IllegalArgumentException( "Diff said to remove " + item + " from " +
                    Arrays.toString( from ) + ", but it didn't contain it" );
        }
    }
}
