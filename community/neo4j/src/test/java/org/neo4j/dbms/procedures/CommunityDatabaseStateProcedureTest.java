/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.procedures;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.CommunityDatabaseState;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.procedure.Context;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.values.AnyValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

class CommunityDatabaseStateProcedureTest
{
    private final TestDatabaseIdRepository idRepository = new TestDatabaseIdRepository();

    @Test
    void shouldThrowWithInvalidInput()
    {
        var procedure = procedure( new StubDatabaseStateService( CommunityDatabaseState::unknown ) );
        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{}, mock( ResourceTracker.class ) ) );

        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{null}, mock( ResourceTracker.class ) ) );

        assertThrows( IllegalArgumentException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{intValue( 42 ),stringValue( "The answer" )}, mock( ResourceTracker.class ) ) );
    }

    @Test
    void shouldThrowWhenDatabaseNotFound() throws ProcedureException
    {
        // given
        var existing = idRepository.getRaw( "existing" );
        var nonExisting = idRepository.getRaw( "nonExisting" );
        idRepository.filter( nonExisting.name() );

        Map<NamedDatabaseId,DatabaseState> states = Map.of( existing, new CommunityDatabaseState( existing, true, false, null ) );
        var stateService = new StubDatabaseStateService( states, CommunityDatabaseState::unknown );
        var procedure = procedure( stateService );

        // when/then

        // Should not throw
        procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( existing.name() )}, mock( ResourceTracker.class ) );
        // Should throw
        assertThrows( ProcedureException.class,
                () -> procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( nonExisting.name() )}, mock( ResourceTracker.class ) ) );
    }

    @Test
    void shouldReturnEmptyErrorForNoError() throws ProcedureException
    {
        // given
        var existing = idRepository.getRaw( "existing" );
        Map<NamedDatabaseId,DatabaseState> states = Map.of( existing, new CommunityDatabaseState( existing, true, false, null ) );
        var stateService = new StubDatabaseStateService( states, CommunityDatabaseState::unknown );
        var procedure = procedure( stateService );

        // when
        var result = procedure.apply( mock( Context.class ), new AnyValue[]{stringValue( existing.name() )}, mock( ResourceTracker.class ) );
        var returned = Arrays.asList( result.next() );

        // then
        assertEquals( 4, returned.size(), "Procedure result should have 4 columns: role, address, state and error message" );

        var roleColumn = stringValue( "standalone" );
        var addressColumn = stringValue( "localhost:7687" );
        var statusColumn = stringValue( "online" );
        var errorColumn = stringValue( "" );
        assertEquals( Arrays.asList( roleColumn, addressColumn, statusColumn, errorColumn ), returned, "Error column should be empty" );
    }

    private StandaloneDatabaseStateProcedure procedure( DatabaseStateService stateService )
    {
        return new StandaloneDatabaseStateProcedure( stateService, idRepository, Config.defaults().get( BoltConnector.advertised_address ).toString() );
    }
}
