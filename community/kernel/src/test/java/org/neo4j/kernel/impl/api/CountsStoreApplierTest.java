/*
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
package org.neo4j.kernel.impl.api;

public class CountsStoreApplierTest
{
//    @Test
//    public void shouldNotifyCacheAccessOnHowManyUpdatesOnCountsWeHadSoFar() throws IOException
//    {
//        // GIVEN
//        final CountsTracker tracker = mock( CountsTracker.class );
//        final CountsAccessor.Updater updater = mock( CountsAccessor.Updater.class );
//        final Optional<CountsTracker.Updater> optionalUpdater = Optional.of( null )
//        when( optionalUpdater.map( any() ) ).thenReturn( Optional.of( updater ) );
//        when( tracker.apply( anyLong() ) ).thenReturn( optionalUpdater );
//        final CountsStoreApplier applier = new CountsStoreApplier( tracker );
//
//        // WHEN
//        applier.visitNodeCountsCommand( addNodeCommand() );
//        applier.apply();
//
//        // THEN
//        verify( updater, times( 1 ) ).incrementNodeCount( ANY_LABEL, 1 );
//    }
//
//    private Command.NodeCountsCommand addNodeCommand()
//    {
//        final Command.NodeCountsCommand command = new Command.NodeCountsCommand();
//        command.init( ANY_LABEL, 1 );
//        return command;
//    }
}
