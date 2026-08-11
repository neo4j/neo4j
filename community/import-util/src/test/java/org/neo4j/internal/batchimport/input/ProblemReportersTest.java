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
package org.neo4j.internal.batchimport.input;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.TRUNCATE_OPTIONS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.common.EntityType;
import org.neo4j.internal.batchimport.input.BadCollector.ProblemReporter;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

@ExtendWith(EphemeralFileSystemExtension.class)
class ProblemReportersTest {

    private static final String DUFF = "duff";
    private static final String SOURCE = "some.file";
    private static final long LINE = 13;
    private static final int COL = 31;

    private static final Groups GROUPS = new Groups();
    private static final Group G1 = GROUPS.getOrCreate("G1");
    private static final Group G2 = GROUPS.getOrCreate("G2");

    private static final String P1 = "p1";
    private static final String P2 = "p2";
    private static final Object V1 = "v1";
    private static final Object V2 = 42;

    private static final AtomicInteger REPORT_ID = new AtomicInteger();

    @Inject
    private FileSystemAbstraction fs;

    @ParameterizedTest
    @MethodSource
    void shouldReportProblems(Problem problem) throws IOException {
        assertPrintedProblem(problem.reporter, problem.expectedPlain);
        assertJsonProblem(problem.reporter, problem.expectedJson);
    }

    private static void assertPrintedProblem(ProblemReporter reporter, String expectedContent) throws IOException {
        try (var output = new ByteArrayOutputStream();
                var handler = ProblemReporters.printingProblemHandler(output)) {
            handler.handle(reporter);
            assertThat(output.toString(StandardCharsets.UTF_8).trim())
                    .isEqualToIgnoringNewLines(expectedContent.trim());
        }
    }

    private void assertJsonProblem(ProblemReporter reporter, String expectedContent) throws IOException {
        Path report = Path.of("report-%d.json.log".formatted(REPORT_ID.incrementAndGet()))
                .toAbsolutePath();
        try (var handler = ProblemReporters.jsonOutputProblemHandler(fs.open(report, TRUNCATE_OPTIONS))) {
            handler.handle(reporter);
        }
        try (var input = fs.openAsInputStream(report)) {
            assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8).trim())
                    .isEqualToIgnoringNewLines(expectedContent.trim());
        }
    }

    private static Stream<Problem> shouldReportProblems() {
        return Stream.of(
                new Problem(
                        ProblemReporters.relationshipsProblemReporter(
                                "node1", G1, "theRel", "node2", G2, DUFF, SOURCE, LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Invalid relationship in import data\\nsome.file: line 13\\n(node1:G1)-[theRel]->(node2:G2) referring to missing node duff",
                                "source":"some.file",
                                "line":13,
                                "relationship":{
                                    "type":"theRel",
                                    "source":{
                                        "id":"node1",
                                        "group":"G1"
                                    },
                                    "target":{
                                        "id":"node2",
                                        "group":"G2"
                                    },
                                    "invalid": "duff"
                                }
                            }
                            """),
                        """
                        Invalid relationship in import data
                        some.file: line 13
                        (node1:G1)-[theRel]->(node2:G2) referring to missing node duff
                        """),
                new Problem(
                        ProblemReporters.relationshipsProblemReporter(
                                null, G1, "theRel", "node2", G2, DUFF, SOURCE, LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Invalid relationship in import data\\nsome.file: line 13\\n(null:G1)-[theRel]->(node2:G2) is missing data",
                                "source":"some.file",
                                "line":13,
                                "relationship":{
                                    "type":"theRel",
                                    "source":{
                                        "id":null,
                                        "group":"G1"
                                    },
                                    "target":{
                                        "id":"node2",
                                        "group":"G2"
                                    },
                                    "missing": true
                                }
                            }
                            """),
                        """
                        Invalid relationship in import data
                        some.file: line 13
                        (null:G1)-[theRel]->(node2:G2) is missing data
                        """),
                new Problem(
                        ProblemReporters.relationshipsProblemReporter(
                                "node1", G1, "theRel", null, G2, DUFF, SOURCE, LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Invalid relationship in import data\\nsome.file: line 13\\n(node1:G1)-[theRel]->(null:G2) is missing data",
                                "source":"some.file",
                                "line":13,
                                "relationship":{
                                    "type":"theRel",
                                    "source":{
                                        "id":"node1",
                                        "group":"G1"
                                    },
                                    "target":{
                                        "id":null,
                                        "group":"G2"
                                    },
                                    "missing": true
                                }
                            }
                            """),
                        """
                        Invalid relationship in import data
                        some.file: line 13
                        (node1:G1)-[theRel]->(null:G2) is missing data
                        """),
                new Problem(
                        ProblemReporters.relationshipsProblemReporter(
                                "node1", G1, null, "node2", G2, DUFF, SOURCE, LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Invalid relationship in import data\\nsome.file: line 13\\n(node1:G1)-[null]->(node2:G2) is missing data",
                                "source":"some.file",
                                "line":13,
                                "relationship":{
                                    "type":null,
                                    "source":{
                                        "id":"node1",
                                        "group":"G1"
                                    },
                                    "target":{
                                        "id":"node2",
                                        "group":"G2"
                                    },
                                    "missing": true
                                }
                            }
                            """),
                        """
                        Invalid relationship in import data
                        some.file: line 13
                        (node1:G1)-[null]->(node2:G2) is missing data
                        """),
                new Problem(
                        ProblemReporters.relationshipViolatingConstraintReporter(
                                Map.of(P1, V1, P2, V2),
                                "CREATE CONSTRAINT Foo",
                                "node1",
                                G1,
                                "theRel",
                                "node2",
                                G2,
                                SOURCE,
                                LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Relationship would have violated a constraint\\nsome.file: line 13\\n(node1:G1)-[theRel]->(node2:G2) would have violated constraint: CREATE CONSTRAINT Foo, with properties:{p1=v1, p2=42}",
                                "source":"some.file",
                                "line":13,
                                "constraint":"CREATE CONSTRAINT Foo",
                                "relationship":{
                                    "type":"theRel",
                                    "source":{
                                        "id":"node1",
                                        "group":"G1"
                                    },
                                    "target":{
                                        "id":"node2",
                                        "group":"G2"
                                    },
                                    "properties":{
                                        "p1":"v1",
                                        "p2":"42"
                                    }
                                }
                            }
                            """),
                        """
                        Relationship would have violated a constraint
                        some.file: line 13
                        (node1:G1)-[theRel]->(node2:G2) would have violated constraint: CREATE CONSTRAINT Foo, with properties:{p1=v1, p2=42}
                        """),
                new Problem(
                        ProblemReporters.entityViolatingConstraintReporter(
                                "node1",
                                13,
                                Map.of(P1, V1, P2, V2),
                                "CREATE CONSTRAINT Foo",
                                EntityType.NODE,
                                SOURCE,
                                LINE),
                        formatJson("""
                            {
                                "problem":"NodeViolation",
                                "message":"Node would have violated a constraint\\nsome.file: line 13\\nNode node1 (internal id 13) would have violated constraint: CREATE CONSTRAINT Foo, with properties: {p1=v1, p2=42}",
                                "source":"some.file",
                                "line":13,
                                "constraint":"CREATE CONSTRAINT Foo",
                                "node":{
                                    "entityId":"node1",
                                    "dbId":"13",
                                    "properties":{
                                        "p1":"v1",
                                        "p2":"42"
                                    }
                                }
                            }
                            """),
                        """
                        Node would have violated a constraint
                        some.file: line 13
                        Node node1 (internal id 13) would have violated constraint: CREATE CONSTRAINT Foo, with properties: {p1=v1, p2=42}
                        """),
                new Problem(
                        ProblemReporters.entityViolatingConstraintReporter(
                                "rel1",
                                13,
                                Map.of(P1, V1, P2, V2),
                                "CREATE CONSTRAINT Foo",
                                EntityType.RELATIONSHIP,
                                SOURCE,
                                LINE),
                        formatJson("""
                            {
                                "problem":"BadRelationship",
                                "message":"Relationship would have violated a constraint\\nsome.file: line 13\\nRelationship rel1 (internal id 13) would have violated constraint: CREATE CONSTRAINT Foo, with properties: {p1=v1, p2=42}",
                                "source":"some.file",
                                "line":13,
                                "constraint":"CREATE CONSTRAINT Foo",
                                "relationship":{
                                    "entityId":"rel1",
                                    "dbId":"13",
                                    "properties":{
                                        "p1":"v1",
                                        "p2":"42"
                                    }
                                }
                            }
                            """),
                        """
                        Relationship would have violated a constraint
                        some.file: line 13
                        Relationship rel1 (internal id 13) would have violated constraint: CREATE CONSTRAINT Foo, with properties: {p1=v1, p2=42}
                        """),
                new Problem(ProblemReporters.nodesProblemReporter("id1", G1, SOURCE, LINE), formatJson("""
                    {
                        "problem":"DuplicateNode",
                        "message":"Id defined multiple times\\nsome.file: line 13\\n'id1' is defined more than once in group 'G1'",
                        "source":"some.file",
                        "line":13,
                        "node":{
                            "id":"id1",
                            "group":"G1"
                        }
                    }
                    """), """
                        Id defined multiple times
                        some.file: line 13
                        'id1' is defined more than once in group 'G1'
                        """),
                new Problem(ProblemReporters.collectExtraColumnsReporter(SOURCE, LINE, DUFF), formatJson("""
                    {
                        "problem":"ExtraColumn",
                        "message":"Row has more columns than expected based on the header.\\nsome.file: line 13\\nBad extra column value: 'duff'.",
                        "source":"some.file",
                        "line":13,
                        "extraValue":"duff"
                    }
                    """), """
                        Row has more columns than expected based on the header.
                        some.file: line 13
                        Bad extra column value: 'duff'.
                        """),
                new Problem(
                        ProblemReporters.schemaCommandFailureReporter(EntityType.NODE, DUFF), formatJson("""
                            {
                                "problem": "NodeSchemaViolation",
                                "message": "duff"
                            }
                            """), DUFF),
                new Problem(
                        ProblemReporters.schemaCommandFailureReporter(EntityType.RELATIONSHIP, DUFF),
                        formatJson("""
                            {
                                "problem": "RelationshipSchemaViolation",
                                "message": "duff"
                            }
                            """),
                        DUFF),
                new Problem(ProblemReporters.otherViolationReporter(EntityType.NODE, DUFF), formatJson("""
                    {
                        "problem": "OtherNodeViolation",
                        "message": "duff"
                    }
                    """), DUFF),
                new Problem(
                        ProblemReporters.otherViolationReporter(EntityType.RELATIONSHIP, DUFF), formatJson("""
                            {
                                "problem": "OtherRelationshipViolation",
                                "message": "duff"
                            }
                            """), DUFF),
                new Problem(ProblemReporters.dataAfterQuoteReporter(SOURCE, LINE, DUFF), formatJson("""
                    {
                        "problem":"DataAfterQuote",
                        "message":"Characters after an ending quote in a CSV field are not supported.\\nsome.file: line 13\\nColumn content: `duff`.",
                        "source":"some.file",
                        "line":13,
                        "value":"duff"
                    }
                    """), """
                        Characters after an ending quote in a CSV field are not supported.
                        some.file: line 13
                        Column content: `duff`.
                        """),
                new Problem(ProblemReporters.illegalQuoteReporter(SOURCE, LINE, DUFF), formatJson("""
                    {
                        "problem":"IllegalQuote",
                        "message":"Quotes are only allowed in quoted strings in a CSV field.\\nsome.file: line 13\\nColumn content: `duff`.",
                        "source":"some.file",
                        "line":13,
                        "value":"duff"
                    }
                    """), """
                        Quotes are only allowed in quoted strings in a CSV field.
                        some.file: line 13
                        Column content: `duff`.
                        """),
                new Problem(
                        ProblemReporters.invalidIdReporter(SOURCE, LINE, DUFF, EntityType.RELATIONSHIP),
                        formatJson("""
                    {
                        "problem":"InvalidRelationshipId",
                        "message":"ID value is invalid for the id type specified.\\nsome.file: line 13\\nInvalid ID value: `duff`.",
                        "source":"some.file",
                        "line":13,
                        "value":"duff"
                    }
                    """),
                        """
                        ID value is invalid for the id type specified.
                        some.file: line 13
                        Invalid ID value: `duff`.
                        """),
                new Problem(ProblemReporters.idColumnMissingReporter(SOURCE, LINE, COL), formatJson("""
                            {
                                "problem": "MissingIdColumn",
                                "message": "Row has fewer columns than expected and ID column is missing.\\nsome.file: line 13\\nMissing ID column index: 31.",
                                "source":"some.file",
                                "line":13,
                                "columnIndex":"31"
                            }
                            """), """
                        Row has fewer columns than expected and ID column is missing.
                        some.file: line 13
                        Missing ID column index: 31.
                        """));
    }

    private static String formatJson(String json) {
        return json
                // strip out all non-content WS
                .replaceAll("\\s\\s+", "")
                // tidy up field separators
                .replace("\": ", "\":")
                // drop all new lines that are left
                .replace("\r", "")
                .replace("\n", "");
    }

    private record Problem(ProblemReporter reporter, String expectedJson, String expectedPlain) {
        @Override
        public String toString() {
            return reporter.typeKey();
        }
    }
}
