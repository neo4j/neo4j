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
package org.neo4j.importer;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_VECTOR_DESCRIPTOR;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.importer.SchemaCommandReader.ReaderConfig;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.VectorType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
public abstract class SchemaCommandReaderTest {
    protected static final Config CONFIG = Config.defaults();

    private static final String INVALID_OPTION_CYPHER_5 =
            "Invalid option provided, valid options are `indexProvider` and `indexConfig`";
    private static final IndexConfig VECTOR_CONFIG = IndexConfig.with(Map.ofEntries(
            entry("vector.similarity_function", Values.utf8Value("COSINE")),
            entry("vector.default_search_expansion_factor", Values.doubleValue(1.5)),
            entry("vector.quantization.type", Values.utf8Value("SCALAR")),
            entry("vector.hnsw.m", Values.intValue(16)),
            entry("vector.hnsw.ef_construction", Values.intValue(100))));
    private static final IndexConfig VECTOR_CONFIG_WITH_DIMENSIONS =
            VECTOR_CONFIG.withIfAbsent("vector.dimensions", Values.intValue(1536));
    private static final IndexConfig POINT_CONFIG =
            IndexConfig.with(Map.of("spatial.cartesian.min", Values.doubleArray(new double[] {0.0, 0.0})));
    private static final IndexConfig FULLTEXT_CONFIG =
            IndexConfig.with(Map.of("fulltext.eventually_consistent", Values.booleanValue(true)));

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    public abstract SchemaCommandReader createReader(FileSystemAbstraction fs, ReaderConfig readerConfig);

    public SchemaCommandReader createReader(ReaderConfig readerConfig) {
        return createReader(fs, readerConfig);
    }

    @Test
    void requiresValidCypherPath() throws IOException {
        final var changeReader = createReader(ReaderConfig.forTesting(false, false, CONFIG));
        assertThatThrownBy(() -> changeReader.parse((Path) null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The path to the Cypher schema commands must exist");

        final var cypher = directory.file("changes");
        assertThatThrownBy(() -> changeReader.parse(cypher))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The path to the Cypher schema commands must exist");

        Files.writeString(cypher, "CYPHER 5 ", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> changeReader.parse(cypher))
                .isInstanceOf(SyntaxException.class)
                .hasMessageContaining("Unexpected end of input");
    }

    @ParameterizedTest
    @MethodSource
    void createsCorrectChanges(String cypherText, List<SchemaCommand> expectedChanges) throws IOException {
        // Might need to be enabled when the next experimental version appear:
        // config.set(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);
        final var reader = createReader(ReaderConfig.forTesting(true, true, CONFIG));
        assertThat(reader.parse(createCypher(cypherText))).containsExactlyElementsOf(expectedChanges);
    }

    @ParameterizedTest
    @MethodSource
    void handlesIncorrectChanges(String cypherText, String... errors) throws IOException {
        final var cypher = createCypher(cypherText);
        // Might need to be enabled when the next experimental version appear:
        // config.set(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, true);
        final var reader = createReader(ReaderConfig.forTesting(true, true, CONFIG));
        assertThatThrownBy(() -> reader.parse(cypher)).hasMessageContainingAll(errors);
    }

    @Test
    void reportsUsefulErrorLocation() throws IOException {
        final var cypher = createCypher("""
                CREATE INDEX testing1
                FOR (n1:LabelName1)
                ON (n1.propertyName);
                CREATE INDEX testing2
                FOR (n2:LabelName2)
                ON (n1.propertyName);
                CREATE INDEX testing3
                   FOR (n:LabelName1)
                   ON (n.propertyName);
                CREATE INDEX testing4
                   FOR (n:LabelName2)
                   ON (n2.propertyName);
                """);
        final var reader = createReader(ReaderConfig.forTesting(true, true, CONFIG));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll(
                        "Unable to parse the Cypher in import change commands.",
                        "Variable `n1` not defined",
                        "line 6, column 5 (offset: 110)",
                        "Variable `n2` not defined",
                        "line 12, column 8 (offset: 247)");
    }

    @ParameterizedTest
    @MethodSource
    void disallowDropIfConfigDenies(String cypherText) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = createReader(ReaderConfig.forTesting(true, false, CONFIG));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("Dropping indexes or constraints is not currently supported");
    }

    @ParameterizedTest
    @MethodSource
    void disallowEnterpriseFeaturesIfConfigDenies(String cypherText) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = createReader(ReaderConfig.forTesting(false, true, CONFIG));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("Enterprise features are not currently supported");
    }

    public Path createCypher(String cypherText) throws IOException {
        final var changes = directory.file("changes");
        Files.writeString(changes, cypherText, StandardCharsets.UTF_8);
        return changes;
    }

    private static Arguments arguments(String cypher, SchemaCommand change) {
        return Arguments.of(cypher, List.of(change));
    }

    private static Arguments arguments(String cypher, String... errors) {
        return Arguments.of(cypher, errors);
    }

    private static Stream<Arguments> disallowDropIfConfigDenies() {
        return Stream.of(Arguments.of("DROP CONSTRAINT testing"), Arguments.of("DROP INDEX testing"));
    }

    private static Stream<Arguments> disallowEnterpriseFeaturesIfConfigDenies() {
        return Stream.of(
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS NODE KEY"),
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS NOT NULL"),
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS :: STRING"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS RELATIONSHIP KEY"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS NOT NULL"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS :: INTEGER"));
    }

    private static Stream<Arguments> createsCorrectChanges() {
        return Stream.of(
                Arguments.of("", List.of()),
                // DROP INDEX
                arguments("DROP INDEX testing1", new IndexCommand.Drop("testing1", false)),
                arguments("DROP INDEX testing2 IF EXISTS", new IndexCommand.Drop("testing2", true)),
                // DROP CONSTRAINT
                arguments("DROP CONSTRAINT testing1", new ConstraintCommand.Drop("testing1", false)),
                arguments("DROP CONSTRAINT testing2 IF EXISTS", new ConstraintCommand.Drop("testing2", true)),
                // NODE RANGE INDEX
                arguments("""
                    CREATE INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeRange(null, "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE RANGE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), true)),
                arguments("""
                    CREATE INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), true)),
                arguments("""
                    CYPHER 5 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CYPHER 25 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { }
                    """, new NodeRange("testing", "LabelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """, new NodeRange("testing", "LabelName", List.of("property1", "property2"), false)),
                // REL RANGE INDEX
                arguments("""
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipRange(null, "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR ()-[r:RelName]->()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR ()<-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR ()<-[r:RelName]->()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE RANGE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), true)),
                arguments("""
                    CREATE INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), true)),
                arguments("""
                    CYPHER 5 CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CYPHER 25 CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { }
                    """, new RelationshipRange("testing", "RelName", List.of("propertyName"), false)),
                arguments("""
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """, new RelationshipRange("testing", "RelName", List.of("property1", "property2"), false)),
                // NODE TEXT INDEX
                arguments("""
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeText("testing", "LabelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeText(null, "LabelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodeText("testing", "LabelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodeText("testing", "LabelName", "propertyName", true)),
                arguments("""
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodeText("testing", "LabelName", "propertyName", true)),
                arguments("""
                    CYPHER 5 CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'text-2.0' }
                    """, new NodeText("testing", "LabelName", "propertyName", false)),
                arguments("""
                    CYPHER 25 CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { }
                    """, new NodeText("testing", "LabelName", "propertyName", false)),
                // REL TEXT INDEX
                arguments("""
                    CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipText("testing", "RelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipText(null, "RelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipText("testing", "RelName", "propertyName", false)),
                arguments("""
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipText("testing", "RelName", "propertyName", true)),
                arguments("""
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipText("testing", "RelName", "propertyName", true)),
                arguments("""
                    CYPHER 5 CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'text-2.0' }
                    """, new RelationshipText("testing", "RelName", "propertyName", false)),
                arguments("""
                    CYPHER 25 CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { }
                    """, new RelationshipText("testing", "RelName", "propertyName", false)),
                // NODE POINT INDEX
                arguments("""
                    CREATE POINT INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodePoint(null, "LabelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodePoint("testing", "LabelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodePoint("testing", "LabelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """, new NodePoint("testing", "LabelName", "propertyName", true, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """, new NodePoint("testing", "LabelName", "propertyName", true, IndexConfig.empty())),
                arguments("""
                    CYPHER 5 CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """, new NodePoint("testing", "LabelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CYPHER 25 CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { }
                    """, new NodePoint("testing", "LabelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexConfig: {`spatial.cartesian.min`:[0.0, 0.0]} }
                    """, new NodePoint("testing", "LabelName", "propertyName", false, POINT_CONFIG)),
                // REL POINT INDEX
                arguments("""
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipPoint("testing", "RelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipPoint(null, "RelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipPoint("testing", "RelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """, new RelationshipPoint("testing", "RelName", "propertyName", true, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """, new RelationshipPoint("testing", "RelName", "propertyName", true, IndexConfig.empty())),
                arguments("""
                    CYPHER 5 CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """, new RelationshipPoint("testing", "RelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CYPHER 25 CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { }
                    """, new RelationshipPoint("testing", "RelName", "propertyName", false, IndexConfig.empty())),
                arguments("""
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexConfig: {`spatial.cartesian.min`:[0.0, 0.0]} }
                    """, new RelationshipPoint("testing", "RelName", "propertyName", false, POINT_CONFIG)),
                // NODE LOOKUP INDEX
                arguments("""
                    CREATE LOOKUP INDEX testing
                    FOR (n)
                    ON EACH labels(n)
                    """, new NodeLookup("testing", false)),
                arguments("""
                    CREATE LOOKUP INDEX
                    FOR (n)
                    ON EACH labels(n)
                    """, new NodeLookup(null, false)),
                arguments("""
                    CREATE LOOKUP INDEX testing IF NOT EXISTS
                    FOR (n)
                    ON EACH labels(n)
                    """, new NodeLookup("testing", true)),
                arguments("""
                    CYPHER 5 CREATE LOOKUP INDEX testing
                    FOR (n)
                    ON EACH labels(n)
                    OPTIONS { indexProvider: 'token-lookup-1.0' }
                    """, new NodeLookup("testing", false)),
                arguments("""
                    CYPHER 25 CREATE LOOKUP INDEX testing
                    FOR (n)
                    ON EACH labels(n)
                    OPTIONS { }
                    """, new NodeLookup("testing", false)),
                // REL LOOKUP INDEX
                arguments("""
                    CREATE LOOKUP INDEX testing
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """, new RelationshipLookup("testing", false)),
                arguments("""
                    CREATE LOOKUP INDEX
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """, new RelationshipLookup(null, false)),
                arguments("""
                    CREATE LOOKUP INDEX testing IF NOT EXISTS
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """, new RelationshipLookup("testing", true)),
                arguments("""
                    CYPHER 5 CREATE LOOKUP INDEX testing
                    FOR ()-[r]-()
                    ON EACH type(r)
                    OPTIONS { indexProvider: 'token-lookup-1.0' }
                    """, new RelationshipLookup("testing", false)),
                arguments("""
                    CYPHER 25 CREATE LOOKUP INDEX testing
                    FOR ()-[r]-()
                    ON EACH type(r)
                    OPTIONS { }
                    """, new RelationshipLookup("testing", false)),
                // NODE FULLTEXT INDEX
                arguments(
                        """
                    CREATE FULLTEXT INDEX
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                null, List.of("LabelName"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                "testing", List.of("LabelName"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                "testing", List.of("LabelName"), List.of("propertyName"), true, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName1|LabelName2)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName1", "LabelName2"),
                                List.of("propertyName"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.property1,n.property2]
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName"),
                                List.of("property1", "property2"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName1|LabelName2)
                    ON EACH [n.property1,n.property2]
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName1", "LabelName2"),
                                List.of("property1", "property2"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    OPTIONS { indexConfig: {`fulltext.eventually_consistent`:true} }
                    """,
                        new NodeFulltext(
                                "testing", List.of("LabelName"), List.of("propertyName"), false, FULLTEXT_CONFIG)),
                arguments(
                        """
                    CYPHER 5 CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    OPTIONS { indexProvider: 'fulltext-1.0' }
                    """,
                        new NodeFulltext(
                                "testing", List.of("LabelName"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CYPHER 25 CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    OPTIONS { }
                    """,
                        new NodeFulltext(
                                "testing", List.of("LabelName"), List.of("propertyName"), false, IndexConfig.empty())),
                // REL FULLTEXT INDEX
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                "testing", List.of("RelType"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                null, List.of("RelType"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                "testing", List.of("RelType"), List.of("propertyName"), true, IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType1|RelType2]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType1", "RelType2"),
                                List.of("propertyName"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.property1,r.property2]
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType"),
                                List.of("property1", "property2"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType1|RelType2]-()
                    ON EACH [r.property1,r.property2]
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType1", "RelType2"),
                                List.of("property1", "property2"),
                                false,
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    OPTIONS { indexConfig: {`fulltext.eventually_consistent`:true} }
                    """,
                        new RelationshipFulltext(
                                "testing", List.of("RelType"), List.of("propertyName"), false, FULLTEXT_CONFIG)),
                arguments(
                        """
                    CYPHER 5 CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    OPTIONS { indexProvider: 'fulltext-1.0' }
                    """,
                        new RelationshipFulltext(
                                "testing", List.of("RelType"), List.of("propertyName"), false, IndexConfig.empty())),
                arguments(
                        """
                    CYPHER 25 CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    OPTIONS { }
                    """,
                        new RelationshipFulltext(
                                "testing", List.of("RelType"), List.of("propertyName"), false, IndexConfig.empty())),
                // NODE VECTOR INDEX
                arguments(
                        """
                    CREATE VECTOR INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeVector(
                                null,
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                true,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CYPHER 5 CREATE VECTOR INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      },
                      indexProvider: 'vector-1.0'
                    }
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR, // ignores legacy provider, uses latest
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)), // and the config parsed as if latest
                arguments(
                        """
                    CYPHER 25 CREATE VECTOR INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      }
                    }
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                // REL VECTOR INDEX
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG)),
                arguments(
                        """
                    CREATE VECTOR INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new RelationshipVector(
                                null,
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                true,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CYPHER 5 CREATE VECTOR INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      },
                      indexProvider: 'vector-2.0'
                    }
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CYPHER 25 CREATE VECTOR INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelName"),
                                "propertyName",
                                List.of(),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                // VECTOR INDEX with multiple labels and additional filter properties
                arguments(
                        """
                    CYPHER 25 CREATE VECTOR INDEX testing
                    FOR (n:LabelOne|LabelTwo)
                    ON (n.embedding)
                    WITH [n.filterOne, n.filterTwo]
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new NodeVector(
                                "testing",
                                List.of("LabelOne", "LabelTwo"),
                                "embedding",
                                List.of("filterOne", "filterTwo"),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                arguments(
                        """
                    CYPHER 25 CREATE VECTOR INDEX testing
                    FOR ()-[r:RelOne|RelTwo]-()
                    ON (r.embedding)
                    WITH [r.filterOne]
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536
                      }
                    }
                    """,
                        new RelationshipVector(
                                "testing",
                                List.of("RelOne", "RelTwo"),
                                "embedding",
                                List.of("filterOne"),
                                DEFAULT_VECTOR_DESCRIPTOR,
                                false,
                                VECTOR_CONFIG_WITH_DIMENSIONS)),
                // constraints
                arguments("""
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """, new NodeExistence(null, "LabelName", "prop", false, false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """, new NodeExistence("testing", "LabelName", "prop", false, false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """, new NodeExistence("testing", "LabelName", "prop", false, true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: STRING
                    """,
                        new NodePropertyType(
                                null, "LabelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false, false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: STRING
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(SchemaValueType.STRING),
                                false,
                                false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: INTEGER
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(SchemaValueType.INTEGER),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<INT8>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.int8Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<INT8 NOT NULL>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.int8Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<INT16>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.int16Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<INT32>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.int32Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<INT64>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.int64Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<FLOAT32>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.float32Vector(128)),
                                false,
                                true)),
                arguments(
                        """
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: VECTOR<FLOAT64>(128)
                    """,
                        new NodePropertyType(
                                "testing",
                                "LabelName",
                                "prop",
                                PropertyTypeSet.of(VectorType.float64Vector(128)),
                                false,
                                true)),
                arguments("""
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """, new NodeKey(null, "LabelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """, new NodeKey("testing", "LabelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """, new NodeKey("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE (c.prop1, c.prop2) IS NODE KEY
                    """, new NodeKey("testing", "LabelName", List.of("prop1", "prop2"), false)),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new NodeKey("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    OPTIONS { }
                    """, new NodeKey("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE UNIQUE
                    """, new NodeUniqueness(null, "LabelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE UNIQUE
                    """, new NodeUniqueness("testing", "LabelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS UNIQUE
                    """, new NodeUniqueness("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE (c.prop1, c.prop2) IS NODE UNIQUE
                    """, new NodeUniqueness("testing", "LabelName", List.of("prop1", "prop2"), false)),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new NodeUniqueness("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { }
                    """, new NodeUniqueness("testing", "LabelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """, new RelationshipExistence(null, "RelName", "prop", false, false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """, new RelationshipExistence("testing", "RelName", "prop", false, false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """, new RelationshipExistence("testing", "RelName", "prop", false, true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: STRING
                    """,
                        new RelationshipPropertyType(
                                null, "RelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false, false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: STRING
                    """,
                        new RelationshipPropertyType(
                                "testing",
                                "RelName",
                                "prop",
                                PropertyTypeSet.of(SchemaValueType.STRING),
                                false,
                                false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: INTEGER
                    """,
                        new RelationshipPropertyType(
                                "testing",
                                "RelName",
                                "prop",
                                PropertyTypeSet.of(SchemaValueType.INTEGER),
                                false,
                                true)),
                arguments("""
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP KEY
                    """, new RelationshipKey(null, "RelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP KEY
                    """, new RelationshipKey("testing", "RelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS REL KEY
                    """, new RelationshipKey("testing", "RelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE (c.prop1, c.prop2) IS REL KEY
                    """, new RelationshipKey("testing", "RelName", List.of("prop1", "prop2"), false)),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS REL KEY
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new RelationshipKey("testing", "RelName", List.of("prop"), true)),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS REL KEY
                    OPTIONS { }
                    """, new RelationshipKey("testing", "RelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP UNIQUE
                    """, new RelationshipUniqueness(null, "RelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP UNIQUE
                    """, new RelationshipUniqueness("testing", "RelName", List.of("prop"), false)),
                arguments("""
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE
                    """, new RelationshipUniqueness("testing", "RelName", List.of("prop"), true)),
                arguments("""
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE (c.prop1, c.prop2) IS REL UNIQUE
                    """, new RelationshipUniqueness("testing", "RelName", List.of("prop1", "prop2"), false)),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { indexProvider: 'range-1.0' }
                    """, new RelationshipUniqueness("testing", "RelName", List.of("prop"), true)),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { }
                    """, new RelationshipUniqueness("testing", "RelName", List.of("prop"), true)),

                // MULTIPLES
                Arguments.of(
                        """
                    DROP INDEX testing1;
                    DROP CONSTRAINT testing2;
                    """,
                        List.of(
                                new IndexCommand.Drop("testing1", false),
                                new ConstraintCommand.Drop("testing2", false))),
                Arguments.of("""
                    DROP INDEX testing1;
                    DROP INDEX testing1;
                    """, List.of(new IndexCommand.Drop("testing1", false))),
                Arguments.of("""
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    DROP INDEX testing;
                    """, List.of()),
                Arguments.of("""
                    CYPHER 5
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    """, List.of(new NodeRange("testing", "LabelName", List.of("propertyName"), false))),
                Arguments.of(
                        """
                    DROP INDEX testing;
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    """,
                        List.of(
                                new IndexCommand.Drop("testing", false),
                                new NodeRange("testing", "LabelName", List.of("propertyName"), false))),
                Arguments.of(
                        """
                    CREATE RANGE INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE TEXT INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propertyName);
                    """,
                        List.of(
                                new NodeRange("testing1", "LabelName", List.of("propertyName"), false),
                                new NodeText("testing2", "LabelName", "propertyName", false))),
                Arguments.of(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propString IS UNIQUE;
                    CREATE INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propInt);
                    """,
                        List.of(
                                new NodeUniqueness("testing1", "LabelName", List.of("propString"), false),
                                new NodeRange("testing2", "LabelName", List.of("propInt"), false))),
                Arguments.of(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n:LabelName)
                    REQUIRE n.propString IS UNIQUE;
                    CREATE INDEX testing2
                    FOR (n:LabelName)
                    ON (n.propInt);
                    """,
                        List.of(
                                new NodeUniqueness("testing1", "LabelName", List.of("propString"), false),
                                new NodeRange("testing2", "LabelName", List.of("propInt"), false))));
    }

    private static Stream<Arguments> handlesIncorrectChanges() {
        return Stream.of(
                // not schema changes
                arguments("RETURN 13", "Only schema change clauses are allowed here but found: SingleQuery"),
                arguments("USE neo4j", "Only schema change clauses are allowed here but found: SingleQuery"),
                arguments(
                        """
                    USE neo4j
                    CREATE INDEX testing
                    FOR (n:LabelName1)
                    ON (n.propertyName);
                    """,
                        "Schema commands are only applied to the database to be imported into so graph names are not allowed: neo4j"),
                // naming
                arguments("""
                    CREATE INDEX testing1
                    FOR (n1:LabelName1)
                    ON (n1.propertyName);
                    CREATE INDEX testing2
                    FOR (n2:LabelName2)
                    ON (n1.propertyName);
                    """, "Variable `n1` not defined"),
                // drop
                arguments("DROP CONSTRAINT $testing", "Parameters are not allowed to be used as a constraint name"),
                arguments(
                        "DROP INDEX boom DROP CONSTRAINT testing2",
                        "Invalid input 'DROP': expected 'IF EXISTS' or <EOF>"),
                // invalid options
                arguments("""
                    CYPHER 5 CREATE TEXT INDEX testing
                    FOR (n:LabelName1)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'vector-1.0' }
                    """, "The provider 'vector-1.0' of type VECTOR does not match the expected type of TEXT"),
                arguments("""
                    CYPHER 5 CREATE INDEX testing
                    FOR (n:LabelName1)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """, "The provider 'point-1.0' of type POINT does not match the expected type of RANGE"),
                arguments("""
                    CYPHER 5 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { provider: 'duff' }
                    """, "Unable to parse the Cypher", INVALID_OPTION_CYPHER_5),
                arguments(
                        """
                    CYPHER 25 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { provider: 'duff' }
                    """,
                        "Unable to parse the Cypher",
                        "22N04: Invalid input 'provider' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments(
                        """
                    CYPHER 5 CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: ['duff'] }
                    """,
                        "Could not create range node property index with specified index provider 'List{String(\"duff\")}'",
                        "Expected String value"),
                arguments("""
                    CYPHER 5 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: 'duff' }
                    """, "Unable to parse the Cypher", INVALID_OPTION_CYPHER_5),
                arguments(
                        """
                    CYPHER 25 CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: 'duff' }
                    """,
                        "Unable to parse the Cypher",
                        "22N04: Invalid input 'config' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CYPHER 5 CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: {go:['boom']} }
                    """, "Unable to parse the Cypher", INVALID_OPTION_CYPHER_5),
                arguments(
                        """
                    CYPHER 25 CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: {go:['boom']} }
                    """,
                        "Unable to parse the Cypher",
                        "22N04: Invalid input 'config' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CYPHER 5 CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'range-1.0', config: ['boom'] }
                    """, "Unable to parse the Cypher", INVALID_OPTION_CYPHER_5),
                arguments(
                        """
                    CYPHER 25 CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: ['boom'] }
                    """,
                        "Unable to parse the Cypher",
                        "22N04: Invalid input 'config' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName, n.propertyName)
                    """, "Invalid range node property index as property 'propertyName' is duplicated"),
                arguments("""
                    CREATE INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName, r.propertyName)
                    """, "Invalid range relationship property index as property 'propertyName' is duplicated"),
                arguments("""
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (x.propertyName)
                    """, "Unable to parse the Cypher", "Variable `x` not defined"),
                arguments("""
                    CREATE INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (x.propertyName)
                    """, "Unable to parse the Cypher", "Variable `x` not defined"),
                arguments("""
                    CREATE TEXT INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """, "Unable to parse the Cypher", "Only single property text indexes are supported"),
                arguments("""
                    CREATE TEXT INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """, "Only single property text indexes are supported"),
                arguments("""
                    CREATE POINT INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """, "Unable to parse the Cypher", "Only single property point indexes are supported"),
                arguments("""
                    CREATE POINT INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """, "Only single property point indexes are supported"),
                arguments("""
                    CREATE VECTOR INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      }
                    }
                    """, "Unable to parse the Cypher", "Only single property vector indexes are supported"),
                arguments("""
                    CREATE VECTOR INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      }
                    }
                    """, "Only single property vector indexes are supported"),
                arguments(
                        """
                    CREATE VECTOR INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        foo: 13
                      }
                    }
                    """,
                        "Could not create vector index with specified index config '{vector.dimensions: 1536, foo: 13}'",
                        "'foo' is an unrecognized setting"),
                arguments("""
                    CREATE LOOKUP INDEX boom
                    FOR (n)
                    ON EACH labels(x)
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE LOOKUP INDEX boom
                    FOR (n)
                    ON EACH foo(n)
                    """, "Function 'foo' is not allowed, valid function is 'labels'"),
                arguments("""
                    CREATE LOOKUP INDEX boom
                    FOR ()-[r]-()
                    ON EACH type(x)
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE FULLTEXT INDEX boom
                    FOR (n:LabelName|LabelName)
                    ON EACH [n.property1, n.property2]
                    """, "Invalid fulltext node index as label 'LabelName' is duplicated"),
                arguments("""
                    CREATE FULLTEXT INDEX boom
                    FOR ()-[r:RelName|RelName]-()
                    ON EACH [r.property1, r.property2]
                    """, "Invalid fulltext relationship index as relationship 'RelName' is duplicated"),
                arguments("""
                    CREATE LOOKUP INDEX boom
                    FOR ()-[r]-()
                    ON EACH foo(n)
                    """, "Function 'foo' is not allowed, valid function is 'type'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS UNIQUE
                    """, "Variable `x` not defined"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """, "Failed to create uniqueness constraint", INVALID_OPTION_CYPHER_5),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS NOT NULL
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE (n.prop1, n.prop2) IS NOT NULL
                    """, "Constraint type 'IS NOT NULL' does not allow multiple properties"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """, "Failed to create node property existence constraint", "Invalid option provided"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'."),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS NODE KEY
                    """, "Variable `x` not defined"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NODE KEY
                    OPTIONS { duff:13 }
                    """, "Failed to create node key constraint", INVALID_OPTION_CYPHER_5),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NODE KEY
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS :: STRING
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE (n.prop1, n.prop2) IS :: STRING
                    """, "Constraint type 'IS TYPED' does not allow multiple properties"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """, "Failed to create node property type constraint", "Invalid option provided"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'."),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: VECTOR<INT16>(666)
                    """, "Invalid input 'VECTOR'"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: VECTOR<INT13>(666)
                    """, "Invalid input '<'"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: VECTOR<NUM16>(666)
                    """, "Invalid input '<'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS UNIQUE
                    """, "Variable `x` not defined"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """, "Failed to create relationship uniqueness constraint", INVALID_OPTION_CYPHER_5),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS NOT NULL
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE (r.prop1, r.prop2) IS NOT NULL
                    """, "Constraint type 'IS NOT NULL' does not allow multiple properties"),
                arguments(
                        """
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """, "Failed to create relationship property existence constraint", "Invalid option provided"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'."),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS REL KEY
                    """, "Variable `x` not defined"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS REL KEY
                    OPTIONS { duff:13 }
                    """, "Failed to create relationship key constraint", INVALID_OPTION_CYPHER_5),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS REL KEY
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS :: STRING
                    """, "Variable `x` not defined"),
                arguments("""
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE (r.prop1, r.prop2) IS :: STRING
                    """, "Constraint type 'IS TYPED' does not allow multiple properties"),
                arguments("""
                    CYPHER 5 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """, "Failed to create relationship property type constraint", "Invalid option provided"),
                arguments("""
                    CYPHER 25 CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """, "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'."),
                // mixed index/constraint violations
                arguments("""
                    CREATE INDEX testing
                    FOR (n1:LabelName1)
                    ON (n1.propertyName);
                    CREATE INDEX testing
                    FOR (n2:LabelName2)
                    ON (n2.propertyName);
                    """, "Multiple operations for the schema command with name testing"),
                arguments("""
                    CREATE INDEX testing
                    FOR (n1:LabelName1)
                    ON (n1.propertyName);
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE;
                    """, "Multiple operations for the schema command with name testing"),
                // lookups
                arguments(
                        """
                    CREATE LOOKUP INDEX testing1 FOR (n1) ON EACH labels(n1);
                    CREATE LOOKUP INDEX testing2 FOR (n2) ON EACH labels(n2);
                    """, "Multiple node lookup indexes found - only 1 is allowed per database: testing1,testing2"),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing1 FOR ()-[r1]-() ON EACH type(r1);
                    CREATE LOOKUP INDEX testing2 FOR ()-[r2]-() ON EACH type(r2);
                    """,
                        "Multiple relationship lookup indexes found - only 1 is allowed per database: testing1,testing2"),
                arguments("""
                    CREATE INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propertyName);
                    """, "An index of type 'RANGE' is also specified - unable to create index 'testing2'"),
                arguments("""
                    CREATE POINT INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE POINT INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propertyName);
                    """, "An index of type 'POINT' is also specified - unable to create index 'testing2'"),
                arguments(
                        """
                    CREATE INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS UNIQUE;
                    """,
                        "Cannot create index 'testing1' as it clashes with the constraint 'testing2' also having a backing index of type 'RANGE'"),
                arguments(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS :: STRING;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS :: INTEGER;
                    """,
                        "A property type constraint of 'STRING' is also specified - unable to create 'testing2' with type 'INTEGER'"),
                arguments("""
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS UNIQUE;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS UNIQUE;
                    """, "Duplicate backing indexes found for constraints 'testing1' and 'testing2'"),
                arguments("""
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS NOT NULL;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS NOT NULL;
                    """, "Duplicate schemas found for constraints 'testing1' and 'testing2'"),
                arguments("""
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS :: STRING;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS :: STRING;
                    """, "Duplicate schemas found for constraints 'testing1' and 'testing2'"));
    }
}
