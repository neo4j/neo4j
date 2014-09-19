/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.util.RelIdArray.DirectionWrapper;

public interface RelationshipLoadingPosition extends CloneableInPublic
{
    void updateFirst( long first );
    
    long position( DirectionWrapper direction, int[] types );
    
    long nextPosition( long position, DirectionWrapper direction, int[] types );
    
    boolean hasMore( DirectionWrapper direction, int[] types );

    void compareAndAdvance( long relIdDeleted, long nextRelId );
    
    @Override
    public RelationshipLoadingPosition clone();
    
    public static final RelationshipLoadingPosition EMPTY = new RelationshipLoadingPosition()
    {
        @Override
        public void updateFirst( long first )
        {
        }
        
        @Override
        public long position( DirectionWrapper direction, int[] types )
        {
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }
        
        @Override
        public long nextPosition( long position, DirectionWrapper direction, int[] types )
        {
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        }
        
        @Override
        public boolean hasMore( DirectionWrapper direction, int[] types )
        {
            return false;
        }

        @Override
        public void compareAndAdvance( long relIdDeleted, long nextRelId )
        {
        }
        
        @Override
        public RelationshipLoadingPosition clone()
        {
            return this;
        }
    };

}
