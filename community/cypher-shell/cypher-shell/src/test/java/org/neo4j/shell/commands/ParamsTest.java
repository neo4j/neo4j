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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parameter.ParameterService.Parameter;
import org.neo4j.shell.printer.Printer;

class ParamsTest {
    private HashMap<String, Parameter> vars;
    private Printer printer;
    private Params cmd;

    @BeforeEach
    void setup() {
        vars = new HashMap<>();
        printer = mock(Printer.class);
        ParameterService parameters = mock(ParameterService.class);
        when(parameters.parameters()).thenReturn(vars);
        cmd = new Params(printer, parameters);
    }

    @Test
    void descriptionNotNull() {
        assertThat(cmd.metadata().description()).isNotNull();
    }

    @Test
    void usageNotNull() {
        assertThat(cmd.metadata().usage()).isNotNull();
    }

    @Test
    void helpNotNull() {
        assertThat(cmd.metadata().help()).isNotNull();
    }

    @Test
    void runCommand() throws CommandException {
        // given
        String var = "var";
        int value = 9;
        addParam("var", 9);
        // when
        cmd.execute(List.of());
        // then
        verify(printer).printOut(":param var => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandAlignment() throws CommandException {
        // given
        addParam("var", 9);
        addParam("param", 99999);
        // when
        cmd.execute(List.of());
        // then
        verify(printer).printOut(":param param => 99999");
        verify(printer).printOut(":param var   => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandWithArg() throws CommandException {
        // given
        addParam("var", 9);
        addParam("param", 9999);
        // when
        cmd.execute(List.of("var"));
        // then
        verify(printer).printOut(":param var => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandWithArgWithExtraSpace() throws CommandException {
        // given
        addParam("var", 9);
        addParam("param", 9999);
        // when
        cmd.execute(List.of(" var"));
        // then
        verify(printer).printOut(":param var => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandWithArgWithBackticks() throws CommandException {
        // given
        addParam("var", 9);
        addParam("param", 9999);
        // when
        cmd.execute(List.of("`var`"));
        // then
        verify(printer).printOut(":param `var` => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandWithSpecialCharacters() throws CommandException {
        // given
        addParam("var `", 9);
        addParam("param", 9999);
        // when
        cmd.execute(List.of("`var ```"));
        // then
        verify(printer).printOut(":param `var ``` => 9");
        verifyNoMoreInteractions(printer);
    }

    @Test
    void runCommandWithUnknownArg() {
        // given
        addParam("var", 9);

        // when
        assertThatThrownBy(() -> cmd.execute(List.of("bob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Unknown parameter: bob");
    }

    @Test
    void shouldNotAcceptMoreThanOneArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob", "sob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    private void addParam(String name, int value) {
        vars.put(name, new Parameter(name, String.valueOf(value), value));
    }
}
