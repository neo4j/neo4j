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
package org.neo4j.shell.parameter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.parameter.ParameterService.RawParameter;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.values.storable.Values;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;

class ShellParameterServiceTest
{
    private ShellParameterService parameters;
    private TransactionHandler transactionHandler;

    @BeforeEach
    void setup()
    {
        transactionHandler = mock( TransactionHandler.class );
        parameters = new ShellParameterService( transactionHandler );
    }

    @Test
    void evaluateOffline() throws CommandException
    {
        assertEvaluate( "'hi'", "hi" );
        assertEvaluate( "\"hello\"", "hello" );
        assertEvaluate( "123", 123L );
        assertEvaluate( "1.01", 1.01 );
        assertEvaluate( "true", true );
        assertEvaluate( "[1, 'hej']", List.of( 1L, "hej" ) );
        assertEvaluate( "{ hej: 1 }", Map.of( "hej", 1L ) );
        assertEvaluate( "true", true );
        assertEvaluate( "point({x: 1.0, y: 2.0})", Values.pointValue( CARTESIAN, 1.0, 2.0 ) );
        assertEvaluate( "date('2021-01-13')", LocalDate.of( 2021, 1, 13 ) );
        assertEvaluate( "datetime('2022-01-13T11:00Z')", ZonedDateTime.of( 2022, 1, 13, 11, 0, 0, 0, ZoneOffset.UTC ) );
        assertEvaluate( "localdatetime('2022-01-13T11:00')", LocalDateTime.of( 2022, 1, 13, 11, 0 ) );
        assertEvaluate( "localtime('12:00')", LocalTime.of( 12, 0 ) );
        assertEvaluate( "duration({ hours: 23 })", Values.durationValue( Duration.ofHours( 23 ) ) );
    }

    @Test
    void evaluateOnline() throws CommandException
    {
        var mockRecord = mock( org.neo4j.driver.Record.class );
        when( mockRecord.get( "result" ) ).thenReturn( new IntegerValue( 6 ) );
        var mockBoltResult = mock( BoltResult.class );
        when( mockBoltResult.iterate() ).thenReturn( List.of( mockRecord ).iterator() );
        when( transactionHandler.runCypher( eq( "RETURN 1 + 2 + 3 AS `result`;" ), any() ) )
                .thenReturn( Optional.of( mockBoltResult ) );

        assertEvaluate( "1 + 2 + 3", 6L );
    }

    @Test
    void failToEvaluate()
    {
        var exception = assertThrows( CommandException.class, () -> parameters.evaluate( new RawParameter( "somename", "INVALID" ) ) );
        assertThat( exception.getMessage(), is( "Failed to evaluate parameter somename: INVALID" ) );
    }

    @Test
    void parse() throws ParameterService.ParameterParsingException
    {
        assertParse( "bob   9", "bob", "9" );
        assertParse( "bob => 9", "bob", "9" );
        assertParse( "`bob` => 9", "bob", "9" );
        assertParse( "bØb   9", "bØb", "9" );
        assertParse( "`first=>Name` => \"Bruce\"", "first=>Name", "\"Bruce\"" );
        assertParse( "`bob#`   9", "bob#", "9" );
        assertParse( " `bo `` sömething ```   9", "bo ` sömething `", "9" );
        assertParse( "bob 'one two'", "bob", "'one two'" );
        assertParse( "böb 'one two'", "böb", "'one two'" );
        assertParse( "bob: \"one\"", "bob", "\"one\"" );
        assertParse( "`bob:`: 'one'", "bob:", "'one'" );
        assertParse( "`t:om` 'two'", "t:om", "'two'" );
        assertParse( "bob \"RETURN 5 as bob\"", "bob", "\"RETURN 5 as bob\"" );
    }

    @Test
    void setParameter()
    {
        var parameter = new Parameter( "key", "'value'", "value" );
        parameters.setParameter( parameter );
        assertThat( parameters.parameters(), is( Map.of( "key", parameter ) ) );
        assertThat( parameters.parameterValues(), is( Map.of( "key", parameter.value() ) ) );
    }

    @Test
    void setExistingParameter()
    {
        parameters.setParameter( new Parameter( "key", "'old'", "old" ) );
        var parameter = new Parameter( "key", "'value'", "value" );
        parameters.setParameter( parameter );
        assertThat( parameters.parameters(), is( Map.of( "key", parameter ) ) );
        assertThat( parameters.parameterValues(), is( Map.of( "key", parameter.value() ) ) );
    }

    @Test
    void setMultipleParameters()
    {
        var parameter1 = new Parameter( "key1", "'value1'", "value1" );
        var parameter2 = new Parameter( "key2", "'value2'", "value2" );

        parameters.setParameter( parameter1 );
        parameters.setParameter( parameter2 );
        assertThat( parameters.parameters(), is( Map.of( "key1", parameter1, "key2", parameter2 ) ) );
        assertThat( parameters.parameterValues(), is( Map.of( "key1", parameter1.value(), "key2", parameter2.value() ) ) );
    }

    private void assertEvaluate( String expression, Object expectedValue ) throws CommandException
    {
        var raw = new RawParameter( "someName", expression );
        var value = parameters.evaluate( raw );
        Supplier<String> message = () -> " Expected " + expectedValue + " but got " + value.value() + " (" + value.value().getClass().getName() + ")";
        assertEquals( new Parameter( raw.name(), raw.expression(), expectedValue ), value, message );
    }

    private void assertParse( String input, String expectedName, String expectedExpression ) throws ParameterService.ParameterParsingException
    {
        var parsed = parameters.parse( input );
        assertThat( parsed, is( new RawParameter( expectedName, expectedExpression ) ) );
    }
}
