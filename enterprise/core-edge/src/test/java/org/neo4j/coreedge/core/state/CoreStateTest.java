/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.core.state;

import org.junit.Test;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.core.state.snapshot.CoreStateDownloader;
import org.neo4j.coreedge.identity.StoreId;
import org.neo4j.coreedge.core.consensus.RaftMessages;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static org.neo4j.coreedge.identity.RaftTestMember.member;

public class CoreStateTest
{
    @Test
    public void shouldLogOnStoreIdMismatchAndNonEmptyStore() throws Exception
    {
        // given
        StoreId localStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId otherStoreId = new StoreId( 5, 6, 7, 8 );

        LocalDatabase localDatabase = mock( LocalDatabase.class );
        when( localDatabase.isEmpty() ).thenReturn( false );
        when( localDatabase.storeId() ).thenReturn( localStoreId );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        CoreState coreState = new CoreState( null, localDatabase, logProvider, null, null, applicationProcess );
        RaftMessages.NewEntry.Request message = new RaftMessages.NewEntry.Request( member( 0 ), null );

        // when
        coreState.handle( new RaftMessages.StoreIdAwareMessage( otherStoreId, message ) );

        // then
        verifyZeroInteractions( applicationProcess );
        logProvider.assertContainsLogCallContaining( "Discarding message" );
    }

    @Test
    public void shouldNotActOnIncomingDefaultStoreIdWithEmptyStore() throws Exception
    {
        // given
        StoreId localStoreId = new StoreId( 1, 2, 3, 4 );
        StoreId otherStoreId = StoreId.DEFAULT;
        RaftMessages.NewEntry.Request message = new RaftMessages.NewEntry.Request( member( 0 ), null );
        CoreStateDownloader downloader = mock( CoreStateDownloader.class );

        LocalDatabase localDatabase = mock( LocalDatabase.class );
        when( localDatabase.isEmpty() ).thenReturn( true );
        when( localDatabase.storeId() ).thenReturn( localStoreId );
        CommandApplicationProcess applicationProcess = mock( CommandApplicationProcess.class );

        CoreState coreState = new CoreState( null, localDatabase, NullLogProvider.getInstance(), null,
                downloader, applicationProcess );

        // when
        coreState.handle( new RaftMessages.StoreIdAwareMessage( otherStoreId, message ) );

        // then
        verifyZeroInteractions( downloader );
    }
}
