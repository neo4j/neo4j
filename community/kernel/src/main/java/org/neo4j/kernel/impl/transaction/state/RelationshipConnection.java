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
package org.neo4j.kernel.impl.transaction.state;

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

enum RelationshipConnection
{
    START_PREV
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.isFirstInFirstChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getFirstPrevRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setFirstPrevRel( id );
            rel.setFirstInFirstChain( isFirst );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return START_NEXT;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getFirstNode();
        }

        @Override
        RelationshipConnection start()
        {
            return this;
        }

        @Override
        RelationshipConnection end()
        {
            return END_PREV;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInFirstChain();
        }
    },
    START_NEXT
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.getFirstNextRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setFirstNextRel( id );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return START_PREV;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getFirstNode();
        }

        @Override
        RelationshipConnection start()
        {
            return this;
        }

        @Override
        RelationshipConnection end()
        {
            return END_NEXT;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInFirstChain();
        }
    },
    END_PREV
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.isFirstInSecondChain() ? Record.NO_NEXT_RELATIONSHIP.intValue() : rel.getSecondPrevRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setSecondPrevRel( id );
            rel.setFirstInSecondChain( isFirst );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return END_NEXT;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getSecondNode();
        }

        @Override
        RelationshipConnection start()
        {
            return START_PREV;
        }

        @Override
        RelationshipConnection end()
        {
            return this;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInSecondChain();
        }
    },
    END_NEXT
    {
        @Override
        long get( RelationshipRecord rel )
        {
            return rel.getSecondNextRel();
        }

        @Override
        void set( RelationshipRecord rel, long id, boolean isFirst )
        {
            rel.setSecondNextRel( id );
        }

        @Override
        RelationshipConnection otherSide()
        {
            return END_PREV;
        }

        @Override
        long compareNode( RelationshipRecord rel )
        {
            return rel.getSecondNode();
        }

        @Override
        RelationshipConnection start()
        {
            return START_NEXT;
        }

        @Override
        RelationshipConnection end()
        {
            return this;
        }

        @Override
        boolean isFirstInChain( RelationshipRecord rel )
        {
            return rel.isFirstInSecondChain();
        }
    };
    
    abstract long get( RelationshipRecord rel );
    
    abstract boolean isFirstInChain( RelationshipRecord rel );

    abstract void set( RelationshipRecord rel, long id, boolean isFirst );
    
    abstract long compareNode( RelationshipRecord rel );
    
    abstract RelationshipConnection otherSide();
    
    abstract RelationshipConnection start();
    
    abstract RelationshipConnection end();
}
