/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.com;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class TestResourcePool
{
    @Test
    public void dontReuseBrokenInstances() throws Exception
    {
        ResourcePool<Something> pool = new ResourcePool<Something>( 10, 5 )
        {
            @Override
            protected Something create()
            {
                return new Something();
            }
            
            @Override
            protected boolean isAlive( Something resource )
            {
                return !resource.closed;
            }
        };
        
        Something somethingFirst = pool.acquire();
        somethingFirst.doStuff();
        pool.release();
        
        Something something = pool.acquire();
        assertEquals( somethingFirst, something );
        something.doStuff();
        something.close();
        pool.release();
        
        Something somethingElse = pool.acquire();
        assertFalse( something == somethingElse );
        somethingElse.doStuff();
    }
    
    private static class Something
    {
        private boolean closed;
        
        public void doStuff() throws Exception
        {
            if ( closed )
            {
                throw new Exception( "Closed" );
            }
        }
        
        public void close()
        {
            this.closed = true;
        }
    }
}
