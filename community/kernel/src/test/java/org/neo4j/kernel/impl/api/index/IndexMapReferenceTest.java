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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.test.Race;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndexMapReferenceTest
{
    @Test
    public void shouldSynchronizeModifications() throws Throwable
    {
        // given
        IndexMapReference ref = new IndexMapReference();
        IndexProxy[] existing = mockedIndexProxies( 5, 0 );
        ref.modify( indexMap ->
        {
            for ( int i = 0; i < existing.length; i++ )
            {
                indexMap.putIndexProxy( i, existing[i] );
            }
            return indexMap;
        } );

        // when
        Race race = new Race();
        for ( int i = 0; i < existing.length; i++ )
        {
            race.addContestant( removeIndexProxy( ref, i ), 1 );
        }
        IndexProxy[] created = mockedIndexProxies( 3, existing.length );
        for ( int i = 0; i < existing.length; i++ )
        {
            race.addContestant( putIndexProxy( ref, existing.length + i, created[i] ), 1 );
        }
        race.go();

        // then
        for ( int i = 0; i < existing.length; i++ )
        {
            assertNull( ref.getIndexProxy( i ) );
        }
        for ( int i = 0; i < created.length; i++ )
        {
            assertSame( created[i], ref.getIndexProxy( existing.length + i ) );
        }
    }

    private Runnable putIndexProxy( IndexMapReference ref, long indexId, IndexProxy proxy )
    {
        return () -> ref.modify( indexMap ->
        {
            indexMap.putIndexProxy( indexId, proxy );
            return indexMap;
        } );
    }

    private Runnable removeIndexProxy( IndexMapReference ref, long indexId )
    {
        return () -> ref.modify( indexMap ->
        {
            indexMap.removeIndexProxy( indexId );
            return indexMap;
        } );
    }

    private IndexProxy[] mockedIndexProxies( int base, int count )
    {
        IndexProxy[] existing = new IndexProxy[count];
        for ( int i = 0; i < count; i++ )
        {
            existing[i] = mock( IndexProxy.class );
            when( existing[i].getDescriptor() ).thenReturn( IndexDescriptorFactory.forLabel( base + i, 1 ) );
        }
        return existing;
    }
}
