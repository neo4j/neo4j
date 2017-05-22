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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.util.Map;

import org.neo4j.time.FakeClock;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.security.auth.AuthenticationResult.AUTH_DISABLED;
import static org.neo4j.helpers.collection.MapUtil.map;

public class TransactionStateMachineTest
{
    @Test
    public void shouldNotWaitWhenNoBookmarkSupplied() throws Exception
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "BEGIN", emptyMap() );

        verify( stateMachineSPI, never() ).awaitUpToDate( anyLong() );
    }

    @Test
    public void shouldAwaitSingleBookmark() throws Exception
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        stateMachine.run( "BEGIN", map( "bookmark", "neo4j:bookmark:v1:tx15" ) );

        verify( stateMachineSPI ).awaitUpToDate( 15 );
    }

    @Test
    public void shouldAwaitMultipleBookmarks() throws Exception
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        Map<String,Object> params = map( "bookmarks", asList(
                "neo4j:bookmark:v1:tx15", "neo4j:bookmark:v1:tx5", "neo4j:bookmark:v1:tx92", "neo4j:bookmark:v1:tx9" )
        );
        stateMachine.run( "BEGIN", params );

        verify( stateMachineSPI ).awaitUpToDate( 92 );
    }

    @Test
    public void shouldAwaitMultipleBookmarksWhenBothSingleAndMultipleSupplied() throws Exception
    {
        TransactionStateMachineSPI stateMachineSPI = mock( TransactionStateMachineSPI.class );
        TransactionStateMachine stateMachine = newTransactionStateMachine( stateMachineSPI );

        Map<String,Object> params = map(
                "bookmark", "neo4j:bookmark:v1:tx42",
                "bookmarks", asList( "neo4j:bookmark:v1:tx47", "neo4j:bookmark:v1:tx67", "neo4j:bookmark:v1:tx45" )
        );
        stateMachine.run( "BEGIN", params );

        verify( stateMachineSPI ).awaitUpToDate( 67 );
    }

    private static TransactionStateMachine newTransactionStateMachine( TransactionStateMachineSPI stateMachineSPI )
    {
        return new TransactionStateMachine( stateMachineSPI, AUTH_DISABLED, new FakeClock() );
    }
}
