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

import static org.assertj.core.api.Assertions.anyOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.shell.Conditions.contains;
import static org.neo4j.shell.util.Versions.majorVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.prettyprint.TablePlanFormatter;
import org.neo4j.shell.state.BoltStateHandler;

class CypherShellVerboseIntegrationTest extends CypherShellIntegrationTest {
    private final StringLinePrinter linePrinter = new StringLinePrinter();

    @BeforeEach
    void setUp() throws Exception {
        linePrinter.clear();
        var printer = new PrettyPrinter(new PrettyConfig(Format.VERBOSE, true, 1000, false));
        var boltHandler = new BoltStateHandler(true);
        var parameters = mock(ParameterService.class);
        shell = new CypherShell(linePrinter, boltHandler, printer, parameters);

        connect("neo");
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            shell.execute(CypherStatement.complete("MATCH (n) DETACH DELETE (n)"));
        } finally {
            shell.disconnect();
        }
    }

    @Test
    void parseDuration() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("RETURN duration({months:0.75})"));

        // then
        assertThat(linePrinter.output()).contains("P22DT19H51M49.5S");
    }

    @Test
    void cypherWithNoReturnStatements() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));

        // then
        assertThat(linePrinter.output()).contains("Added 1 nodes, Set 1 properties, Added 1 labels");
    }

    @Test
    void cypherWithReturnStatements() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("CREATE (jane :TestPerson {name: \"Jane Smith\"}) RETURN jane"));

        // then
        String output = linePrinter.output();
        assertThat(output)
                .contains("| jane ")
                .contains("| (:TestPerson {name: \"Jane Smith\"}) |")
                .contains("Added 1 nodes, Set 1 properties, Added 1 labels");
    }

    @Test
    void connectTwiceThrows() {
        assertThat(shell.isConnected()).as("Shell should already be connected").isTrue();

        assertThatThrownBy(() -> connect("neo"))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Already connected");
    }

    @Test
    void resetOutOfTxScenario() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));
        shell.reset();

        // then
        shell.execute(CypherStatement.complete("CREATE (:TestPerson {name: \"Jane Smith\"})"));
        shell.execute(CypherStatement.complete("MATCH (n:TestPerson) RETURN n ORDER BY n.name"));

        String result = linePrinter.output();
        assertThat(result)
                .contains("| (:TestPerson {name: \"Jane Smith\"}) |\n" + "| (:TestPerson {name: \"Jane Smith\"}) |");
    }

    @Test
    void cypherWithOrder() throws CommandException {
        // given
        assumeTrue(runningAtLeast("4.1"));

        // Make sure we are creating a new NEW index
        shell.execute(StatementParser.CypherStatement.complete("DROP INDEX ages IF EXISTS"));

        shell.execute(CypherStatement.complete("CREATE INDEX ages FOR (n:Person) ON (n.age)"));
        shell.execute(CypherStatement.complete("CALL db.awaitIndexes()"));

        // when
        var q =
                "CYPHER RUNTIME=INTERPRETED EXPLAIN MATCH (n:Person) WHERE n.age >= 18 RETURN n.name, n.age ORDER BY n.age";
        shell.execute(CypherStatement.complete(q));

        // then
        String actual = linePrinter.output();
        assertThat(actual).contains("Order").contains("n.age ASC");
    }

    @Test
    void cypherWithQueryDetails() throws CommandException {
        // given
        assumeTrue(runningAtLeast("4.1"));

        // when
        shell.execute(CypherStatement.complete("EXPLAIN MATCH (n) with n.age AS age RETURN age"));

        // then
        String actual = linePrinter.output();
        assertThat(actual)
                .contains(TablePlanFormatter.DETAILS, "n.age AS age")
                .doesNotContain(TablePlanFormatter.IDENTIFIERS);
    }

    @Test
    void cypherWithoutQueryDetails() throws CommandException {
        // given
        assumeTrue(!runningAtLeast("4.1"));

        // when
        shell.execute(CypherStatement.complete("EXPLAIN MATCH (n) with n.age AS age RETURN age"));

        // then
        String actual = linePrinter.output();
        assertThat(actual).contains(TablePlanFormatter.IDENTIFIERS).doesNotContain(TablePlanFormatter.DETAILS);
    }

    @Test
    void cypherWithExplainAndRulePlanner() throws CommandException {
        // given (there is no rule planner in neo4j 4.0)
        assumeTrue(majorVersion(shell.getServerVersion()) < 4);

        // when
        shell.execute(CypherStatement.complete(
                "CYPHER planner=rule EXPLAIN MATCH (e:E) WHERE e.bucket='Live' and e.id = 23253473 RETURN count(e)"));

        // then
        String actual = linePrinter.output();
        assertThat(actual)
                .contains("\"EXPLAIN\"")
                .contains("\"READ_ONLY\"")
                .contains("\"RULE\"")
                .contains("\"INTERPRETED\"");
    }

    @Test
    void cypherWithProfileWithMemory() throws CommandException {
        // given
        // Memory profile are only available from 4.1
        assumeTrue(runningAtLeast("4.1"));

        // when
        shell.execute(
                CypherStatement.complete("CYPHER RUNTIME=INTERPRETED PROFILE UNWIND [1,1,2] AS x RETURN DISTINCT x"));

        // then
        String actual = linePrinter.output();
        // First table
        assertThat(actual.replace(" ", ""))
                .contains("|Plan|Statement|Version|Planner|Runtime|Time|DbHits|Rows|Memory(Bytes)|");
        // Second table
        String upTo5_0 = "|Operator|Details|EstimatedRows|Rows|DBHits|Memory(Bytes)|PageCacheHits/Misses|";
        String from5_1 = "|Operator|Id|Details|EstimatedRows|Rows|DBHits|Memory(Bytes)|PageCacheHits/Misses|";
        assertThat(actual.replace(" ", "")).is(anyOf(contains(upTo5_0), contains(from5_1)));
    }

    @Test
    void shouldShowTheNumberOfRows() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("UNWIND [1,2,3] AS row RETURN row"));

        // then
        String actual = linePrinter.output();
        assertThat(actual).contains("3 rows\n");
    }

    @Test
    void shouldNotContainUnnecessaryNewLines() throws CommandException {
        // when
        shell.execute(CypherStatement.complete("UNWIND [1,2,3] AS row RETURN row"));

        // then
        String actual = linePrinter.output();
        assertThat(actual)
                .contains(String.format("+-----+%n" + "| row |%n"
                        + "+-----+%n"
                        + "| 1   |%n"
                        + "| 2   |%n"
                        + "| 3   |%n"
                        + "+-----+%n"
                        + "%n"
                        + "3 rows%n"
                        + "ready to start consuming query after"));
    }
}
