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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;

interface GBPTreeVisitor<KEY,VALUE> extends IdProvider.IdProviderVisitor
{
    void meta( Meta meta );

    void treeState( Pair<TreeState,TreeState> statePair );

    void beginLevel( int level );

    void beginNode( long pageId, boolean isLeaf, long generation, int keyCount );

    void key( KEY key, boolean isLeaf );

    void value( VALUE value );

    void child( long child );

    void position( int i );

    void endNode( long pageId );

    void endLevel( int level );

    class Adaptor<KEY,VALUE> implements GBPTreeVisitor<KEY,VALUE>
    {
        @Override
        public void meta( Meta meta )
        {
        }

        @Override
        public void treeState( Pair<TreeState,TreeState> statePair )
        {
        }

        @Override
        public void beginLevel( int level )
        {
        }

        @Override
        public void beginNode( long pageId, boolean isLeaf, long generation, int keyCount )
        {
        }

        @Override
        public void key( KEY key, boolean isLeaf )
        {
        }

        @Override
        public void value( VALUE value )
        {
        }

        @Override
        public void child( long child )
        {
        }

        @Override
        public void position( int i )
        {
        }

        @Override
        public void endNode( long pageId )
        {
        }

        @Override
        public void endLevel( int level )
        {
        }

        @Override
        public void beginFreelistPage( long pageId )
        {
        }

        @Override
        public void endFreelistPage( long pageId )
        {
        }

        @Override
        public void freelistEntry( long pageId, long generation, int pos )
        {
        }
    }
}
