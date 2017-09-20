/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

class RelationshipTraversalCursor extends RelationshipCursor
        implements org.neo4j.internal.kernel.api.RelationshipTraversalCursor
{
    private long originNodeReference;
    private long next;
    private Record buffer;

    RelationshipTraversalCursor( Read read )
    {
        super( read );
    }

    /*
     * Cursor being called as a group, use the buffered records in Record
     * instead.
     */
    void buffered( long nodeReference, Record record )
    {
        this.originNodeReference = nodeReference;
        this.buffer = Record.initialize( record );
    }

    /*
     * Normal traversal.
     */
    void chain( long nodeReference, long reference )
    {
        setId( NO_ID );
        originNodeReference = nodeReference;
        next = reference;
    }

    /*
     * Reference to a group record
     */
    void groups( long nodeReference, long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void neighbour( NodeCursor cursor )
    {
        read.singleNode( neighbourNodeReference(), cursor );
    }

    @Override
    public long neighbourNodeReference()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return target;
        }
        else if ( target == originNodeReference )
        {
            return source;
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN" );
        }
    }

    @Override
    public long originNodeReference()
    {
        return originNodeReference;
    }

    @Override
    public boolean next()
    {
        if ( hasBufferedData() )
        {   //We have buffered data, iterate the chain of buffered records
            buffer = buffer.next;
            if ( !hasBufferedData() )
            {
                close();
                return false;
            }
            else
            {
                // Copy buffer data to self
                this.setId( buffer.id );
                this.setType( buffer.type );
                this.setNextProp( buffer.nextProp );
                this.setFirstNode( buffer.firstNode );
                this.setSecondNode( buffer.secondNode );
                return true;
            }
        }


        if ( next == NO_ID )
        {
            close();
            return false;
        }
        read.relationship( this, next );
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            next = getFirstNextRel();
        }
        else if ( target == originNodeReference )
        {
            next = getSecondNextRel();
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN" );
        }
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        setId( next = NO_ID );
        buffer = null;
    }

    private boolean hasBufferedData()
    {
        return buffer != null;
    }

    /*
     * Record is both a data holder for buffering data from a RelationshipRecord
     * as well as a linked list over the records in the group.
     */
    static class Record
    {
        private static final RelationshipRecord DUMMY = null;
        final long id;
        final int type;
        final long nextProp;
        final long firstNode;
        final long secondNode;
        final Record next;

        /*
         * Initialize the chain of records
         */
        static Record initialize(Record first)
        {
            return new Record( DUMMY, first );
        }

        /*
         * Initialize the record chain or push a new record as the new head of the record chain
         */
        Record( RelationshipRecord record, Record next )
        {
            if ( record != null )
            {
                id = record.getId();
                type = record.getType();
                nextProp = record.getNextProp();
                firstNode = record.getFirstNode();
                secondNode = record.getSecondNode();
            }
            else
            {
                id = NO_ID;
                type = NO_ID;
                nextProp = NO_ID;
                firstNode = NO_ID;
                secondNode = NO_ID;
            }
            this.next = next;
        }
    }
}
