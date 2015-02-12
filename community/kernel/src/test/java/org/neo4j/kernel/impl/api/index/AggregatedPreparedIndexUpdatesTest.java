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
package org.neo4j.kernel.impl.api.index;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.kernel.api.index.PreparedIndexUpdates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AggregatedPreparedIndexUpdatesTest
{
    @Test
    public void shouldCommitAllAggregates() throws Exception
    {
        // Given
        PreparedIndexUpdates[] aggregates = {preparedUpdatesMock(), preparedUpdatesMock(), preparedUpdatesMock()};
        AggregatedPreparedIndexUpdates indexChanges = new AggregatedPreparedIndexUpdates( aggregates );

        // When
        indexChanges.commit();

        // Then
        for ( PreparedIndexUpdates aggregate : aggregates )
        {
            verify( aggregate ).commit();
        }
    }

    @Test
    public void shouldRollbackAllAggregates()
    {
        // Given
        PreparedIndexUpdates[] aggregates = {preparedUpdatesMock(), preparedUpdatesMock(), preparedUpdatesMock()};
        AggregatedPreparedIndexUpdates indexChanges = new AggregatedPreparedIndexUpdates( aggregates );

        // When
        indexChanges.rollback();

        // Then
        for ( PreparedIndexUpdates aggregate : aggregates )
        {
            verify( aggregate ).rollback();
        }
    }

    @Test
    public void shouldRethrowExceptionFromAggregateCommit() throws Exception
    {
        // Given
        PreparedIndexUpdates[] aggregates = {preparedUpdatesMock(), preparedUpdatesMock()};
        IOException ioException = new IOException( "Disk is full" );
        doThrow( ioException ).when( aggregates[1] ).commit();
        AggregatedPreparedIndexUpdates indexChanges = new AggregatedPreparedIndexUpdates( aggregates );

        // When
        try
        {
            indexChanges.commit();
            fail( "Should have failed with " + IOException.class.getSimpleName() );
        }
        catch ( IOException e )
        {
            // Then
            assertEquals( e, ioException );
        }
    }

    @Test
    public void shouldRollbackNotCommittedAggregatesIfOneCommitFails() throws Exception
    {
        // Given
        PreparedIndexUpdates[] aggregates = {preparedUpdatesMock(), preparedUpdatesMock(), preparedUpdatesMock()};
        doThrow( IOException.class ).when( aggregates[1] ).commit();
        AggregatedPreparedIndexUpdates indexChanges = new AggregatedPreparedIndexUpdates( aggregates );

        // When
        try
        {
            indexChanges.commit();
        }
        catch ( IOException ignored )
        {
            // ok - one of commits failed
        }

        // Then
        verify( aggregates[0] ).commit();
        verifyNoMoreInteractions( aggregates[0] );

        verify( aggregates[1] ).commit();
        verify( aggregates[1] ).rollback();
        verifyNoMoreInteractions( aggregates[1] );

        verify( aggregates[2] ).rollback();
        verifyNoMoreInteractions( aggregates[2] );
    }

    private static PreparedIndexUpdates preparedUpdatesMock()
    {
        return mock( PreparedIndexUpdates.class );
    }
}
