/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Arrays;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import static java.util.Arrays.copyOf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.impl.transaction.XidImpl.DEFAULT_SEED;
import static org.neo4j.kernel.impl.transaction.XidImpl.getNewGlobalId;

public class XidImplTest
{
    @Test
    public void fixedSeedDifferentLocalIdShouldMakeUnequal() throws Exception
    {
        // WHEN
        byte[] globalIdZero = getNewGlobalId( fixedSeed, 0 );
        byte[] globalIdOne = getNewGlobalId( fixedSeed, 1 );
        
        // THEN
        assertFalse( Arrays.equals( globalIdZero, globalIdOne ) );
    }
    
    @Test
    public void fixedSeedSameLocalIdShouldMakeEqual() throws Exception
    {
        // WHEN
        byte[] globalIdZero = getNewGlobalId( fixedSeed, 0 );
        byte[] otherGlobalIdZero = getNewGlobalId( fixedSeed, 0 );
        
        // THEN
        assertTrue( Arrays.equals( globalIdZero, otherGlobalIdZero ) );
    }
    
    @Test
    public void defaultSeedDifferentLocalIdShouldMakeUnequal() throws Exception
    {
        // WHEN
        byte[] globalIdZero = getNewGlobalId( DEFAULT_SEED, 0 );
        byte[] globalIdOne = getNewGlobalId( DEFAULT_SEED, 1 );
        
        // THEN
        assertFalse( Arrays.equals( globalIdZero, globalIdOne ) );
    }
    
    @Test
    public void defaultSeedSameLocalIdShouldMakeUnequal() throws Exception
    {
        // WHEN
        byte[] globalIdZero = getNewGlobalId( DEFAULT_SEED, 0 );
        byte[] otherGlobalIdZero = getNewGlobalId( DEFAULT_SEED, 0 );
        
        // THEN
        assertFalse( Arrays.equals( globalIdZero, otherGlobalIdZero ) );
    }
    
    @Test
    public void shouldProduceShortXidToStringForOldGlobalIdFormat() throws Exception
    {
        // GIVEN
        XidImpl xid = new XidImpl( shorten( getNewGlobalId( DEFAULT_SEED, 10 ), 4 ), "test".getBytes() );
        
        // WHEN
        String toString = xid.toString();
        
        // THEN
        assertThat( toString, CoreMatchers.containsString( "NEOKERNL" ) );
        assertEquals( 2, occurencesOf( '|', toString ) );
    }

    @Test
    public void shouldProduceLongerXidToStringForNewGlobalIdFormat() throws Exception
    {
        // GIVEN
        XidImpl xid = new XidImpl( getNewGlobalId( DEFAULT_SEED, 10 ), "test".getBytes() );
        
        // WHEN
        String toString = xid.toString();
        
        // THEN
        assertThat( toString, CoreMatchers.containsString( "NEOKERNL" ) );
        assertEquals( 3, occurencesOf( '|', toString ) );
    }
    
    private byte[] shorten( byte[] newGlobalId, int byHowMuch )
    {
        return copyOf( newGlobalId, newGlobalId.length-byHowMuch );
    }
    
    private int occurencesOf( char c, String inString )
    {
        int count = 0;
        for ( int i = 0; i < inString.length(); i++ )
        {
            if ( c == inString.charAt( i ) )
            {
                count++;
            }
        }
        return count;
    }

    /*
     * The fixed seed is not a normal seed, it mimics many seed instances having the exact same start condition,
     * for example if two database instances would start and by chance get the same seed.
     */
    private final XidImpl.Seed fixedSeed = new XidImpl.Seed()
    {
        @Override
        public long nextSequenceId()
        {
            return 1;
        }
        
        @Override
        public long nextRandomLong()
        {
            return 14; // soo random
        }
    };
}
