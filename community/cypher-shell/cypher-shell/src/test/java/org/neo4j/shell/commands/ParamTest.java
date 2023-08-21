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
package org.neo4j.shell.commands;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Values;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.printer.Printer;

class ParamTest {
    private TransactionHandler db;
    private ParameterService parameters;
    private Printer printer;
    private Command cmd;

    @BeforeEach
    void setup() {
        db = mock(TransactionHandler.class);
        printer = mock(Printer.class);
        parameters = ParameterService.create(db);
        cmd = new Param(printer, parameters);
    }

    @Test
    void setParams() throws CommandException {
        var param1 = param("myParam", "here I am");
        var param2 = param("myParam2", 2L);
        var param3 = param("myParam", "again");
        assertExecute("myParam => 'here I am'", param1);
        assertExecute("myParam2 => 2", param1, param2);
        assertExecute("myParam => 'again'", param2, param3);
    }

    @Test
    void setParamsMapSyntax() throws CommandException {
        var param1 = param("myParam", "here I am");
        var param2 = param("myParam2", 2L);
        var param3 = param("myParam", "again");
        assertExecute("{myParam: 'here I am'}", param1);
        assertExecute("  {myParam2 :  2  }  ", param1, param2);
        assertExecute("{myParam:'again'}", param2, param3);
    }

    @Test
    void listParams() throws CommandException {
        final var params =
                """
            {
              int: 1,
              duration: duration({hours: 1}),
              string: 'hello',
              otherString: "hi",
              escapedString: '\\'yo\\'',
              bool: true,
              list: [1, 'hello', true],
              map: {a:1,b:true,c:duration('PT2S')},
              ` backticks `: 12
            }
            """;
        cmd.execute(List.of(params));
        cmd.execute(List.of());

        verify(printer)
                .printOut(
                        """
            {
              ` backticks `: 12,
              bool: true,
              duration: duration('PT1H'),
              escapedString: '\\'yo\\'',
              int: 1,
              list: [1, 'hello', true],
              map: {
                a: 1,
                b: true,
                c: duration('PT2S')
              },
              otherString: 'hi',
              string: 'hello'
            }""");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void listParams2() throws CommandException {
        cmd.execute(List.of("{a:1}"));
        cmd.execute(List.of("list"));

        verify(printer).printOut("""
            {
              a: 1
            }""");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void clearParams() throws CommandException {
        cmd.execute(List.of("{a:1,b:[2,2],c:{cc:1}}"));
        assertFalse(parameters.parameters().isEmpty());
        cmd.execute(List.of());
        verify(printer)
                .printOut(
                        """
                    {
                      a: 1,
                      b: [2, 2],
                      c: {
                        cc: 1
                      }
                    }""");

        cmd.execute(List.of("clear"));
        cmd.execute(List.of());
        verify(printer).printOut("""
                    {
                    }""");
        assertTrue(parameters.parameters().isEmpty());
        verifyNoMoreInteractions(printer);
    }

    @Test
    void shouldFailIfOneArg() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage");
    }

    @Test
    void shouldFailForVariablesWithoutEscaping() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob#   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage");
    }

    @Test
    void shouldFailForVariablesMixingMapStyleAssignmentAndLambdas() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob: => 9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage");
    }

    @Test
    void shouldFailForEmptyVariables() {
        assertThatThrownBy(() -> cmd.execute(List.of("``   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage:");
    }

    @Test
    void shouldFailForInvalidVariables() {
        assertThatThrownBy(() -> cmd.execute(List.of("`   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.")
                .hasMessageContaining(":param clear - Clears all parameters")
                .hasMessageContaining(":param {a: 1} - Sets the parameter a to value 1");
    }

    @Test
    void shouldFailForVariablesWithoutText() {
        assertThatThrownBy(() -> cmd.execute(List.of("```   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage:");
    }

    @Test
    void printUsage() {
        String usage = cmd.metadata().usage();
        assertThat(usage).contains(":param {a: 1} - Sets the parameter a to value 1");
        assertThat(usage).contains("The arrow syntax `:param a => 1` is also supported");
    }

    private void assertExecute(String args, Parameter... expected) throws CommandException {
        cmd.execute(List.of(args));
        var expectedValues = stream(expected).collect(toMap(Parameter::name, Parameter::value));
        assertThat(parameters.parameters()).isEqualTo(expectedValues);
    }

    private Parameter param(String name, Object value) {
        return new Parameter(name, Values.value(value));
    }
}
