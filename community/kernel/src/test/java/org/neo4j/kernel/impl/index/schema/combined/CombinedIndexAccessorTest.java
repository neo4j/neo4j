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

import org.junit.Before;
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
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyCombinedThrowIfBothThrow;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyFailOnSingleCloseFailure;
import static org.neo4j.kernel.impl.index.schema.combined.CombinedIndexTestHelp.verifyOtherIsClosedOnSingleThrow;

public class CombinedIndexAccessorTest
{
    private IndexAccessor boostAccessor;
    private IndexAccessor fallbackAccessor;
    private CombinedIndexAccessor combinedIndexAccessor;

    @Before
    public void setup()
    {
        boostAccessor = mock( IndexAccessor.class );
        fallbackAccessor = mock( IndexAccessor.class );
        combinedIndexAccessor = new CombinedIndexAccessor( boostAccessor, fallbackAccessor );
    }

    /* drop */

    @Test
    public void dropMustDropBoostAndFallback() throws Exception
    {
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
        // when
        verifyFailOnSingleDropFailure( boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void dropMustThrowIfDropFallbackFail() throws Exception
    {
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

    /* close */

    @Test
    public void closeMustCloseBoostAndFallback() throws Exception
    {
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
        verifyFailOnSingleCloseFailure( fallbackAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustThrowIfBoostThrow() throws Exception
    {
        verifyFailOnSingleCloseFailure( boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustCloseBoostIfFallbackThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( fallbackAccessor, boostAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustCloseFallbackIfBoostThrow() throws Exception
    {
        verifyOtherIsClosedOnSingleThrow( boostAccessor, fallbackAccessor, combinedIndexAccessor );
    }

    @Test
    public void closeMustThrowIfBothFail() throws Exception
    {
        verifyCombinedThrowIfBothThrow( boostAccessor, fallbackAccessor, combinedIndexAccessor );
    }
}
