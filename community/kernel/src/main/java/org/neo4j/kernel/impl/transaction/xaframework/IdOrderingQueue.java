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
package org.neo4j.kernel.impl.transaction.xaframework;

/**
 * A queue of (long) ids that gets offered ids, in order, although potentially with gaps in between.
 * A thread can come in a wait for the head element to be a certain id, and after it has got it and made
 * whatever needs doing removes the head. Removing the head will notify the next potential thread awaiting
 * another head id.
 *
 * @author Mattias Persson
 */
public interface IdOrderingQueue
{
    void offer( long id );
    
    void awaitHead( long id );
    
    void removeHead( long expectedId );
    
    boolean isEmpty();
    
    public static final IdOrderingQueue BYPASS = new IdOrderingQueue()
    {
        @Override
        public void removeHead( long expectedId )
        {   // Just ignore, it's fine
        }
        
        @Override
        public void offer( long id )
        {   // Just ignore, it's fine
        }
        
        @Override
        public void awaitHead( long id )
        {   // Just ignore, it's fine
        }
        
        @Override
        public boolean isEmpty()
        {
            return true;
        }
    };
}
