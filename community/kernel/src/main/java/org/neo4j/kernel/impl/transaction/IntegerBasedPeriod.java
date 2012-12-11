/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

public class IntegerBasedPeriod implements Period
{
    private int epochNumber = 0;
    
    private Epoch current = next();
    
    private Epoch next()
    {
        return new IntegerBasedEpoch( epochNumber++ );
    }
    
    /* There is a chance that threads calling currentEpoch() would see the previous
     * epoch if it hasn't been through a memory barrier since it asked about the
     * current epoch the last time. It's somewhat OK though since it will only mean
     * a failing transaction, instead of a transaction being able to commit when
     * it shouldn't be able to
     */
    @Override
    public Epoch currentEpoch()
    {
        return current;
    }

    @Override
    public synchronized Epoch nextEpoch()
    {
        return (current = next());
    }
    
    private static class IntegerBasedEpoch implements Epoch
    {
        private final int epochNumber;
        
        private IntegerBasedEpoch( int number )
        {
            epochNumber = number;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + epochNumber;
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            IntegerBasedEpoch other = (IntegerBasedEpoch) obj;
            if ( epochNumber != other.epochNumber )
                return false;
            return true;
        }
    }
}
