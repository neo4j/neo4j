/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.parameter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.TransactionHandler.TransactionType.USER_TRANSPILED;
import static org.neo4j.values.storable.CoordinateReferenceSystem.CARTESIAN;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.parameter.ParameterService.RawParameters;
import org.neo4j.shell.state.BoltResult;

class ShellParameterServiceTest {
    private ShellParameterService parameters;
    private TransactionHandler transactionHandler;

    @BeforeEach
    void setup() {
        transactionHandler = mock(TransactionHandler.class);
        parameters = new ShellParameterService(transactionHandler);
    }

    @Test
    void evaluateOffline() throws CommandException {
        assertEvaluate("'hi'", "hi");
        assertEvaluate("\"hello\"", "hello");
        assertEvaluate("123", 123L);
        assertEvaluate("1.01", 1.01);
        assertEvaluate("true", true);
        assertEvaluate("[1, 'hej']", List.of(1L, "hej"));
        assertEvaluate("{ hej: 1 }", Map.of("hej", 1L));
        assertEvaluate("true", true);
        assertEvaluate("point({x: 1.0, y: 2.0})", Values.point(CARTESIAN.getCode(), 1.0, 2.0));
        assertEvaluate("duration({ hours: 23 })", Values.isoDuration(0, 0, 60 * 60 * 23, 0));
        assertEvaluate("duration('PT1S')", Values.isoDuration(0, 0, 1, 0));
        assertEvaluate("duration('P1M1W1DT1H1.001S')", Values.isoDuration(1, 8, 60 * 60 + 1, 1000000));
        assertEvaluate("[{b:[{id:1}]}]", List.of(Map.of("b", List.of(Map.of("id", 1L)))));
        assertEvaluate(
                "[null, {b:[{id:null}]}]", List.of(NullValue.NULL, Map.of("b", List.of(Map.of("id", NullValue.NULL)))));
        assertEvaluate("null", NullValue.NULL);
    }

    @Test
    void evaluateOnline() throws CommandException {
        var mockRecord = mock(org.neo4j.driver.Record.class);
        var result = new MapValue(Map.of("hello", new IntegerValue(6L)));
        when(mockRecord.get("result")).thenReturn(result);
        var mockBoltResult = mock(BoltResult.class);
        when(mockBoltResult.iterate()).thenReturn(List.of(mockRecord).iterator());
        when(transactionHandler.runCypher(eq("RETURN {hello:1 + 2 + 3} AS `result`"), any(), eq(USER_TRANSPILED)))
                .thenReturn(Optional.of(mockBoltResult));

        assertEvaluate("1 + 2 + 3", new IntegerValue(6L));
    }

    @Test
    void failToEvaluate() {
        var exception = assertThrows(
                ParameterService.ParameterEvaluationException.class,
                () -> parameters.evaluate(new RawParameters("INVALID")));
        assertThat(exception).hasMessageContaining("Failed to evaluate expression INVALID");
    }

    @Test
    void parse() throws ParameterService.ParameterParsingException {
        final var tests = List.of(
                List.of("bob   9", "bob", "9"),
                List.of("bob => 9", "bob", "9"),
                List.of("`bob` => 9", "`bob`", "9"),
                List.of("bØb   9", "bØb", "9"),
                List.of("`first=>Name` => \"Bruce\"", "`first=>Name`", "\"Bruce\""),
                List.of("`bob#`   9", "`bob#`", "9"),
                List.of(" `bo `` sömething ```   9", "`bo `` sömething ```", "9"),
                List.of("bob 'one two'", "bob", "'one two'"),
                List.of("böb 'one two'", "böb", "'one two'"),
                List.of("bob: \"one\"", "bob", "\"one\""),
                List.of("`bob:`: 'one'", "`bob:`", "'one'"),
                List.of("`t:om` 'two'", "`t:om`", "'two'"),
                List.of("bob \"RETURN 5 as bob\"", "bob", "\"RETURN 5 as bob\""));
        for (var test : tests) {
            assertParse(test.get(0), test.get(1), test.get(2));
            assertParse(test.get(0) + ";", test.get(1), test.get(2));
        }
    }

    @Test
    void setParameter() {
        var parameter = param("key", "value");
        parameters.setParameters(List.of(parameter));
        assertThat(parameters.parameters()).isEqualTo(Map.of("key", parameter.value()));
    }

    @Test
    void setExistingParameter() {
        parameters.setParameters(List.of(param("key", "old")));
        var parameter = param("key", "value");
        parameters.setParameters(List.of(parameter));
        assertThat(parameters.parameters()).isEqualTo(Map.of("key", parameter.value()));
    }

    @Test
    void setMultipleParameters() {
        var parameter1 = param("key1", "value1");
        var parameter2 = param("key2", "value2");

        parameters.setParameters(List.of(parameter1));
        parameters.setParameters(List.of(parameter2));
        assertThat(parameters.parameters()).isEqualTo(Map.of("key1", parameter1.value(), "key2", parameter2.value()));
    }

    private void assertEvaluate(String expression, Object expectedValue) throws CommandException {
        var rawParameters = new RawParameters(String.format("{hello:%s}", expression));
        var expected = param("hello", expectedValue);
        assertThat(parameters.evaluate(rawParameters)).containsExactly(expected);
    }

    private void assertParse(String input, String expectedName, String expectedExpression)
            throws ParameterService.ParameterParsingException {
        assertThat(parameters.parse(input))
                .isEqualTo(new RawParameters(String.format("{%s: %s}", expectedName, expectedExpression)));
    }

    private Parameter param(String name, Object value) {
        return new Parameter(name, org.neo4j.driver.Values.value(value));
    }
}
