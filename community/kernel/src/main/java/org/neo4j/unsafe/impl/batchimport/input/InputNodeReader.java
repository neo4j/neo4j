/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.Arrays;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;

import static org.neo4j.unsafe.impl.batchimport.input.InputCache.END_OF_LABEL_CHANGES;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.HAS_LABEL_FIELD;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_ADDITION;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_REMOVAL;
import static org.neo4j.unsafe.impl.batchimport.input.InputCache.LABEL_TOKEN;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_LABELS;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntity.NO_PROPERTIES;

/**
 * Reads cached {@link InputNode} previously stored using {@link InputNodeCacher}.
 */
public class InputNodeReader extends InputEntityReader<InputNode>
{
    public InputNodeReader( StoreChannel channel, StoreChannel header, int bufferSize, Runnable closeAction,
            int maxNbrOfProcessors ) throws IOException
    {
        super( channel, header, bufferSize, closeAction, maxNbrOfProcessors );
    }

    @Override
    protected InputNode readNextOrNull( Object properties, ProcessorState state ) throws IOException
    {
        ReadableClosablePositionAwareChannel channel = state.batchChannel;

        // group
        Group group = readGroup( 0, state );

        // id
        Object id = readValue( channel );

        // labels (diff from previous node)
        byte labelsMode = channel.get();
        Object labels;
        if ( labelsMode == HAS_LABEL_FIELD )
        {
            labels = channel.getLong();
        }
        else if ( labelsMode == END_OF_LABEL_CHANGES )
        {   // Same as for previous node
            labels = state.previousLabels;
        }
        else
        {
            String[] newLabels = state.previousLabels.clone();
            int cursor = newLabels.length;
            while ( labelsMode != END_OF_LABEL_CHANGES )
            {
                switch ( labelsMode )
                {
                case LABEL_REMOVAL: remove( (String) readToken( LABEL_TOKEN, channel ), newLabels, cursor-- ); break;
                case LABEL_ADDITION:
                    (newLabels = ensureRoomForOneMore( newLabels, cursor ))[cursor++] =
                    (String) readToken( LABEL_TOKEN, channel ); break;
                default: throw new IllegalArgumentException( "Unrecognized label mode " + labelsMode );
                }
                labelsMode = channel.get();
            }
            labels = state.previousLabels = cursor == newLabels.length ? newLabels : Arrays.copyOf( newLabels, cursor );
        }

        return new InputNode( sourceDescription(), lineNumber(), position(),
                group, id,
                properties.getClass().isArray() ? (Object[]) properties : NO_PROPERTIES,
                properties.getClass().isArray() ? null : (Long) properties,
                labels.getClass().isArray() ? (String[]) labels : NO_LABELS,
                labels.getClass().isArray() ? null : (Long) labels );
    }

    private String[] ensureRoomForOneMore( String[] labels, int cursor )
    {
        return cursor >= labels.length ? Arrays.copyOf( labels, cursor+1 ) : labels;
    }

    private void remove( String item, String[] from, int cursor )
    {
        for ( int i = 0; i < cursor; i++ )
        {
            if ( item.equals( from[i] ) )
            {
                from[i] = from[cursor-1];
                from[cursor-1] = null;
                return;
            }
        }
        throw new IllegalArgumentException( "Diff said to remove " + item + " from " +
                    Arrays.toString( from ) + ", but it didn't contain it" );
    }
}
