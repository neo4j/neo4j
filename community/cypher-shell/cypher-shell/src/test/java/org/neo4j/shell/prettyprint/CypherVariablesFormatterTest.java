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
package org.neo4j.shell.prettyprint;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.shell.prettyprint.CypherVariablesFormatter.escape;
import static org.neo4j.shell.prettyprint.CypherVariablesFormatter.unescapedCypherVariable;

import org.junit.jupiter.api.Test;

class CypherVariablesFormatterTest {

    @Test
    void escapeNonAlphanumericStrings() {
        assertThat(escape("abc12_A"), is("abc12_A"));
        assertThat(escape("Åbc12_A"), is("Åbc12_A"));
        assertThat(escape("\0"), is("`\0`"));
        assertThat(escape("\n"), is("`\n`"));
        assertThat(escape("comma, separated"), is("`comma, separated`"));
        assertThat(escape("escaped content `back ticks #"), is("`escaped content ``back ticks #`"));
        assertThat(escape("escaped content two `back `ticks"), is("`escaped content two ``back ``ticks`"));
    }

    @Test
    void reEscapeNonAlphanumericStrings() throws Exception {
        assertThat(unescapedCypherVariable("abc12_A"), is("abc12_A"));
        assertThat(unescapedCypherVariable("Åbc12_A"), is("Åbc12_A"));
        assertThat(unescapedCypherVariable("`\0`"), is("\0"));
        assertThat(unescapedCypherVariable("`\n`"), is("\n"));
        assertThat(unescapedCypherVariable("`comma, separated`"), is("comma, separated"));
        assertThat(unescapedCypherVariable("`escaped content ``back ticks #`"), is("escaped content `back ticks #"));
        assertThat(
                unescapedCypherVariable("`escaped content two ``back ``ticks`"),
                is("escaped content two `back `ticks"));
    }
}
