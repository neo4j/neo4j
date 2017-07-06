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
package org.neo4j.kernel.impl.index.schema.combined;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.index.IndexAccessor;

import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class CombinedIndexAccessorTest
{
    @Test
    public void dropMustDropBoostAndFallback() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        // when
        // ... both drop successful
        combinedIndexAccessor.drop();
        // then
        verify( boostAccessor, times( 1 ) ).drop();
        verify( fallbackAccessor, times( 1 ) ).drop();
    }

    @Test
    public void dropMustThrowIfDropBoostFail() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        // when
        verifyFailOnSingleDropFailure( boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropFallbackFail() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        // when
        verifyFailOnSingleDropFailure( fallbackAccessor, combinedIndexAccessor );
    }

    private void verifyFailOnSingleDropFailure( IndexAccessor failingAccessor, CombinedIndexAccessor combinedIndexAccessor )
            throws IOException
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( failingAccessor ).drop();
        try
        {
            combinedIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertSame( expectedFailure, e );
        }
    }

    @Test
    public void dropMustThrowIfBothFail() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );
        IOException boostFailure = new IOException( "boost" );
        IOException fallbackFailure = new IOException( "fallback" );
        doThrow( boostFailure ).when( boostAccessor ).drop();
        doThrow( fallbackFailure ).when( fallbackAccessor ).drop();

        try
        {
            // when
            combinedIndexAccessor.drop();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then
            assertThat( e, anyOf( sameInstance( boostFailure ), sameInstance( fallbackFailure ) ) );
        }
    }

    @Test
    public void closeMustCloseBoostAndFallback() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        // when
        // ... both drop successful
        combinedIndexAccessor.close();
        // then
        verify( boostAccessor, times( 1 ) ).close();
        verify( fallbackAccessor, times( 1 ) ).close();
    }

    @Test
    public void closeMustThrowIfFallbackThrow() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        verifyFailOnSingleCloseFailure( fallbackAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustThrowIfBoostThrow() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );

        verifyFailOnSingleCloseFailure( boostAccessor, combinedIndexAccessor );
    }

    private void verifyFailOnSingleCloseFailure( IndexAccessor failingAccessor, CombinedIndexAccessor combinedIndexAccessor )
            throws IOException
    {
        IOException expectedFailure = new IOException( "fail" );
        doThrow( expectedFailure ).when( failingAccessor ).close();
        try
        {
            combinedIndexAccessor.close();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            assertSame( expectedFailure, e );
        }
    }

    @Test
    public void closeMustThrowIfBothFail() throws Exception
    {
        // given
        IndexAccessor boostAccessor = mock( IndexAccessor.class );
        IndexAccessor fallbackAccessor = mock( IndexAccessor.class );
        CombinedIndexAccessor combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );
        IOException boostFailure = new IOException( "boost" );
        IOException fallbackFailure = new IOException( "fallback" );
        doThrow( boostFailure ).when( boostAccessor ).close();
        doThrow( fallbackFailure ).when( fallbackAccessor ).close();

        try
        {
            // when
            combinedIndexAccessor.close();
            fail( "Should have failed" );
        }
        catch ( IOException e )
        {
            // then0
            assertThat( e, anyOf( sameInstance( boostFailure ), sameInstance( fallbackFailure ) ) );
        }
    }
}
