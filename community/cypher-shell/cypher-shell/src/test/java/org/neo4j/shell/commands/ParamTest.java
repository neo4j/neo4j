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
package org.neo4j.shell.commands;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.Parameter;

class ParamTest {
    private TransactionHandler db = mock(TransactionHandler.class);
    private ParameterService parameters = ParameterService.create(db);
    private Command cmd = new Param(parameters);

    @BeforeEach
    void setup() {
        db = mock(TransactionHandler.class);
        parameters = ParameterService.create(db);
        cmd = new Param(parameters);
    }

    @Test
    void setParams() throws CommandException {
        var param1 = new Parameter("myParam", "'here I am'", "here I am");
        var param2 = new Parameter("myParam2", "2", 2L);
        var param3 = new Parameter("myParam", "'again'", "again");
        assertExecute("myParam => 'here I am'", param1);
        assertExecute("myParam2 => 2", param1, param2);
        assertExecute("myParam => 'again'", param2, param3);
    }

    @Test
    void shouldFailIfNoArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of()))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void shouldFailIfOneArg() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage: :param name => <Cypher Expression>");
    }

    @Test
    void shouldFailForVariablesWithoutEscaping() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob#   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage: :param name => <Cypher Expression>");
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
                .hasMessageContaining("Incorrect usage.\nusage: :param name => <Cypher Expression>");
    }

    @Test
    void shouldFailForInvalidVariables() {
        assertThatThrownBy(() -> cmd.execute(List.of("`   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage: :param name => <Cypher Expression>");
    }

    @Test
    void shouldFailForVariablesWithoutText() {
        assertThatThrownBy(() -> cmd.execute(List.of("```   9")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect usage.\nusage: :param name => <Cypher Expression>");
    }

    @Test
    void printUsage() {
        String usage = cmd.metadata().usage();
        assertThat(usage).contains("name => <Cypher Expression>");
    }

    private void assertExecute(String args, Parameter... expected) throws CommandException {
        cmd.execute(List.of(args));
        var expectedMap = stream(expected).collect(toMap(Parameter::name, identity()));
        assertThat(parameters.parameters()).isEqualTo(expectedMap);
        var expectedValues = stream(expected).collect(toMap(Parameter::name, Parameter::value));
        assertThat(parameters.parameterValues()).isEqualTo(expectedValues);
    }
}
