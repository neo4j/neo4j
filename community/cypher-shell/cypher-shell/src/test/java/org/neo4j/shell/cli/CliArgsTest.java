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
package org.neo4j.shell.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.parameter.ParameterService;

class CliArgsTest {
    private final CliArgs cliArgs = new CliArgs();

    @Test
    void setUsername() {
        cliArgs.setUsername("foo", "bar");
        assertEquals("foo", cliArgs.getUsername());

        cliArgs.setUsername(null, "bar");
        assertEquals("bar", cliArgs.getUsername());
    }

    @Test
    void setPassword() {
        cliArgs.setPassword("foo", "bar");
        assertEquals("foo", cliArgs.getPassword());

        cliArgs.setPassword(null, "bar");
        assertEquals("bar", cliArgs.getPassword());
    }

    @Test
    void setFailBehavior() {
        // default
        assertEquals(FailBehavior.FAIL_FAST, cliArgs.getFailBehavior());

        cliArgs.setFailBehavior(FailBehavior.FAIL_AT_END);
        assertEquals(FailBehavior.FAIL_AT_END, cliArgs.getFailBehavior());
    }

    @Test
    void getNumSampleRows() {
        assertEquals(1000, CliArgs.DEFAULT_NUM_SAMPLE_ROWS);
        assertEquals(CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows());

        cliArgs.setNumSampleRows(null);
        assertEquals(CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows());

        cliArgs.setNumSampleRows(0);
        assertEquals(CliArgs.DEFAULT_NUM_SAMPLE_ROWS, cliArgs.getNumSampleRows());

        cliArgs.setNumSampleRows(120);
        assertEquals(120, cliArgs.getNumSampleRows());
    }

    @Test
    void setFormat() {
        // default
        assertEquals(Format.AUTO, cliArgs.getFormat());

        cliArgs.setFormat(Format.PLAIN);
        assertEquals(Format.PLAIN, cliArgs.getFormat());

        cliArgs.setFormat(Format.VERBOSE);
        assertEquals(Format.VERBOSE, cliArgs.getFormat());
    }

    @Test
    void setCypher() {
        // default
        assertFalse(cliArgs.getCypher().isPresent());

        cliArgs.setCypher("foo");
        assertTrue(cliArgs.getCypher().isPresent());
        assertEquals("foo", cliArgs.getCypher().get());

        cliArgs.setCypher(null);
        assertFalse(cliArgs.getCypher().isPresent());
    }

    @Test
    void getParameters() {
        var list = List.of(new ParameterService.RawParameters("{bla: 'bla'}"));
        cliArgs.setParameters(list);
        assertEquals(list, cliArgs.getParameters());
    }

    @Test
    void setInputFile() {
        cliArgs.setInputFilename("foo");
        assertEquals("foo", cliArgs.getInputFilename());
    }
}
