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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.SyntaxException;
import org.neo4j.importer.SchemaCommandReader.ReaderConfig;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.IndexProviderDescriptor;
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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Values;

@TestDirectoryExtension
class SchemaCommandReaderTest {

    private static final IndexConfig VECTOR_CONFIG_V1 =
            IndexConfig.with(Map.of("vector.similarity_function", Values.stringValue("COSINE")));
    private static final IndexConfig VECTOR_DIMENSIONS_V1 =
            VECTOR_CONFIG_V1.withIfAbsent("vector.dimensions", Values.intValue(1536));
    private static final IndexConfig VECTOR_CONFIG_V2 = IndexConfig.with(Map.of(
            "vector.hnsw.ef_construction",
            Values.intValue(100),
            "vector.hnsw.m",
            Values.intValue(16),
            "vector.quantization.enabled",
            Values.booleanValue(true),
            "vector.similarity_function",
            Values.stringValue("COSINE")));
    private static final IndexConfig VECTOR_DIMENSIONS_V2 =
            VECTOR_CONFIG_V2.withIfAbsent("vector.dimensions", Values.intValue(1536));
    private static final IndexConfig POINT_CONFIG =
            IndexConfig.with(Map.of("spatial.cartesian.min", Values.doubleArray(new double[] {0.0, 0.0})));
    private static final IndexConfig FULLTEXT_CONFIG =
            IndexConfig.with(Map.of("fulltext.eventually_consistent", Values.booleanValue(true)));

    private static final VectorIndexVersion VECTOR_INDEX_VERSION =
            VectorIndexVersion.latestSupportedVersion(KernelVersion.getLatestVersion(Config.defaults()));

    private static final IndexProviderDescriptor LOOKUP = new IndexProviderDescriptor("token-lookup", "1.0");
    private static final IndexProviderDescriptor RANGE = new IndexProviderDescriptor("range", "1.0");
    private static final IndexProviderDescriptor TEXT = new IndexProviderDescriptor("text", "2.0");
    private static final IndexProviderDescriptor POINT = new IndexProviderDescriptor("point", "1.0");
    private static final IndexProviderDescriptor FULLTEXT = new IndexProviderDescriptor("fulltext", "1.0");
    private static final IndexProviderDescriptor VECTOR_V1 = new IndexProviderDescriptor("vector", "1.0");
    private static final IndexProviderDescriptor VECTOR_V2 = new IndexProviderDescriptor("vector", "2.0");

    @Inject
    private TestDirectory directory;

    @Inject
    private FileSystemAbstraction fs;

    @Test
    void requiresValidCypherPath() throws IOException {
        final var changeReader = new SchemaCommandReader(fs, Config.defaults(), ReaderConfig.defaults());
        assertThatThrownBy(() -> changeReader.parse(null))
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

    @ParameterizedTest()
    @MethodSource
    void createsCorrectChanges(String cypherText, List<SchemaCommand> expectedChanges) throws IOException {
        final var reader = new SchemaCommandReader(
                fs, Config.defaults(), new ReaderConfig(true, true, true, VECTOR_INDEX_VERSION));
        assertThat(reader.parse(createCypher(cypherText))).isEqualTo(expectedChanges);
    }

    @ParameterizedTest
    @MethodSource
    void handlesIncorrectChanges(String cypherText, String... errors) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = new SchemaCommandReader(
                fs, Config.defaults(), new ReaderConfig(true, true, true, VECTOR_INDEX_VERSION));
        assertThatThrownBy(() -> reader.parse(cypher)).hasMessageContainingAll(errors);
    }

    @ParameterizedTest
    @MethodSource
    void disallowDropIfConfigDenies(String cypherText) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = new SchemaCommandReader(
                fs, Config.defaults(), new ReaderConfig(true, true, false, VECTOR_INDEX_VERSION));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("Dropping indexes or constraints is not currently supported");
    }

    @ParameterizedTest
    @MethodSource
    void disallowConstraintIfConfigDenies(String cypherText) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = new SchemaCommandReader(
                fs, Config.defaults(), new ReaderConfig(true, false, true, VECTOR_INDEX_VERSION));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("Constraint commands are not currently supported");
    }

    @ParameterizedTest
    @MethodSource
    void disallowEnterpriseFeaturesIfConfigDenies(String cypherText) throws IOException {
        final var cypher = createCypher(cypherText);
        final var reader = new SchemaCommandReader(
                fs, Config.defaults(), new ReaderConfig(false, true, true, VECTOR_INDEX_VERSION));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("Enterprise features are not currently supported");
    }

    private Path createCypher(String cypherText) throws IOException {
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

    private static Stream<Arguments> disallowConstraintIfConfigDenies() {
        return Stream.of(
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS NOT NULL"),
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS NODE KEY"),
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS UNIQUE"),
                Arguments.of("CREATE CONSTRAINT book_isbn FOR (b:Book) REQUIRE b.isbn IS :: STRING"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS NOT NULL"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS RELATIONSHIP KEY"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS UNIQUE"),
                Arguments.of("CREATE CONSTRAINT part_of FOR ()-[p:PART_OF]-() REQUIRE p.sku IS :: INTEGER"));
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
                arguments(
                        """
                    CREATE INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeRange(null, "LabelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE RANGE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), true, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), true, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new NodeRange("testing", "LabelName", List.of("propertyName"), false, Optional.of(RANGE))),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """,
                        new NodeRange(
                                "testing", "LabelName", List.of("property1", "property2"), false, Optional.empty())),
                // REL RANGE INDEX
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange(null, "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()-[r:RelName]->()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()<-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()<-[r:RelName]->()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE RANGE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), false, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), true, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipRange("testing", "RelName", List.of("propertyName"), true, Optional.empty())),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new RelationshipRange(
                                "testing", "RelName", List.of("propertyName"), false, Optional.of(RANGE))),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """,
                        new RelationshipRange(
                                "testing", "RelName", List.of("property1", "property2"), false, Optional.empty())),
                // NODE TEXT INDEX
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeText("testing", "LabelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeText(null, "LabelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeText("testing", "LabelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeText("testing", "LabelName", "propertyName", true, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeText("testing", "LabelName", "propertyName", true, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'text-2.0' }
                    """,
                        new NodeText("testing", "LabelName", "propertyName", false, Optional.of(TEXT))),
                // REL TEXT INDEX
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipText("testing", "RelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipText(null, "RelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipText("testing", "RelName", "propertyName", false, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipText("testing", "RelName", "propertyName", true, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipText("testing", "RelName", "propertyName", true, Optional.empty())),
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'text-2.0' }
                    """,
                        new RelationshipText("testing", "RelName", "propertyName", false, Optional.of(TEXT))),
                // NODE POINT INDEX
                arguments(
                        """
                    CREATE POINT INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodePoint(null, "LabelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodePoint(
                                "testing", "LabelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodePoint(
                                "testing", "LabelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodePoint(
                                "testing", "LabelName", "propertyName", true, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodePoint(
                                "testing", "LabelName", "propertyName", true, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """,
                        new NodePoint(
                                "testing",
                                "LabelName",
                                "propertyName",
                                false,
                                Optional.of(POINT),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexConfig: {`spatial.cartesian.min`:[0.0, 0.0]} }
                    """,
                        new NodePoint("testing", "LabelName", "propertyName", false, Optional.empty(), POINT_CONFIG)),
                // REL POINT INDEX
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipPoint(
                                null, "RelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", false, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", true, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", true, Optional.empty(), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", false, Optional.of(POINT), IndexConfig.empty())),
                arguments(
                        """
                    CREATE POINT INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS { indexConfig: {`spatial.cartesian.min`:[0.0, 0.0]} }
                    """,
                        new RelationshipPoint(
                                "testing", "RelName", "propertyName", false, Optional.empty(), POINT_CONFIG)),
                // NODE LOOKUP INDEX
                arguments(
                        """
                    CREATE LOOKUP INDEX testing
                    FOR (n)
                    ON EACH labels(n)
                    """,
                        new NodeLookup("testing", false, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX
                    FOR (n)
                    ON EACH labels(n)
                    """,
                        new NodeLookup(null, false, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing IF NOT EXISTS
                    FOR (n)
                    ON EACH labels(n)
                    """,
                        new NodeLookup("testing", true, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing
                    FOR (n)
                    ON EACH labels(n)
                    OPTIONS { indexProvider: 'token-lookup-1.0' }
                    """,
                        new NodeLookup("testing", false, Optional.of(LOOKUP))),
                // REL LOOKUP INDEX
                arguments(
                        """
                    CREATE LOOKUP INDEX testing
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """,
                        new RelationshipLookup("testing", false, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """,
                        new RelationshipLookup(null, false, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing IF NOT EXISTS
                    FOR ()-[r]-()
                    ON EACH type(r)
                    """,
                        new RelationshipLookup("testing", true, Optional.empty())),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing
                    FOR ()-[r]-()
                    ON EACH type(r)
                    OPTIONS { indexProvider: 'token-lookup-1.0' }
                    """,
                        new RelationshipLookup("testing", false, Optional.of(LOOKUP))),
                // NODE FULLTEXT INDEX
                arguments(
                        """
                    CREATE FULLTEXT INDEX
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                null,
                                List.of("LabelName"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing IF NOT EXISTS
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName"),
                                List.of("propertyName"),
                                true,
                                Optional.empty(),
                                IndexConfig.empty())),
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
                                Optional.empty(),
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
                                Optional.empty(),
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
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    OPTIONS { indexConfig: {`fulltext.eventually_consistent`:true} }
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                FULLTEXT_CONFIG)),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR (n:LabelName)
                    ON EACH [n.propertyName]
                    OPTIONS { indexProvider: 'fulltext-1.0' }
                    """,
                        new NodeFulltext(
                                "testing",
                                List.of("LabelName"),
                                List.of("propertyName"),
                                false,
                                Optional.of(FULLTEXT),
                                IndexConfig.empty())),
                // REL FULLTEXT INDEX
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                null,
                                List.of("RelType"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing IF NOT EXISTS
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType"),
                                List.of("propertyName"),
                                true,
                                Optional.empty(),
                                IndexConfig.empty())),
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
                                Optional.empty(),
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
                                Optional.empty(),
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
                                Optional.empty(),
                                IndexConfig.empty())),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    OPTIONS { indexConfig: {`fulltext.eventually_consistent`:true} }
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType"),
                                List.of("propertyName"),
                                false,
                                Optional.empty(),
                                FULLTEXT_CONFIG)),
                arguments(
                        """
                    CREATE FULLTEXT INDEX testing
                    FOR ()-[r:RelType]-()
                    ON EACH [r.propertyName]
                    OPTIONS { indexProvider: 'fulltext-1.0' }
                    """,
                        new RelationshipFulltext(
                                "testing",
                                List.of("RelType"),
                                List.of("propertyName"),
                                false,
                                Optional.of(FULLTEXT),
                                IndexConfig.empty())),
                // NODE VECTOR INDEX
                arguments(
                        """
                    CREATE VECTOR INDEX
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    """,
                        new NodeVector(null, "LabelName", "propertyName", false, Optional.empty(), VECTOR_CONFIG_V2)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS {}
                    """,
                        new NodeVector(
                                "testing", "LabelName", "propertyName", false, Optional.empty(), VECTOR_CONFIG_V2)),
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
                                "testing", "LabelName", "propertyName", false, Optional.empty(), VECTOR_DIMENSIONS_V2)),
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
                                "testing", "LabelName", "propertyName", true, Optional.empty(), VECTOR_DIMENSIONS_V2)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
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
                                "LabelName",
                                "propertyName",
                                false,
                                Optional.of(VECTOR_V1),
                                VECTOR_DIMENSIONS_V1)),
                // REL VECTOR INDEX
                arguments(
                        """
                    CREATE VECTOR INDEX testing
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName)
                    OPTIONS {}
                    """,
                        new RelationshipVector(
                                "testing", "RelName", "propertyName", false, Optional.empty(), VECTOR_CONFIG_V2)),
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
                                null, "RelName", "propertyName", false, Optional.empty(), VECTOR_DIMENSIONS_V2)),
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
                                "testing", "RelName", "propertyName", false, Optional.empty(), VECTOR_DIMENSIONS_V2)),
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
                                "testing", "RelName", "propertyName", true, Optional.empty(), VECTOR_DIMENSIONS_V2)),
                arguments(
                        """
                    CREATE VECTOR INDEX testing
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
                                "RelName",
                                "propertyName",
                                false,
                                Optional.of(VECTOR_V2),
                                VECTOR_DIMENSIONS_V2)),
                // constraints
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new NodeExistence(null, "LabelName", "prop", false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new NodeExistence("testing", "LabelName", "prop", false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new NodeExistence("testing", "LabelName", "prop", true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: STRING
                    """,
                        new NodePropertyType(
                                null, "LabelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: STRING
                    """,
                        new NodePropertyType(
                                "testing", "LabelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS :: INTEGER
                    """,
                        new NodePropertyType(
                                "testing", "LabelName", "prop", PropertyTypeSet.of(SchemaValueType.INTEGER), true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """,
                        new NodeKey(null, "LabelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """,
                        new NodeKey("testing", "LabelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    """,
                        new NodeKey("testing", "LabelName", List.of("prop"), true, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE (c.prop1, c.prop2) IS NODE KEY
                    """,
                        new NodeKey("testing", "LabelName", List.of("prop1", "prop2"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE KEY
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new NodeKey("testing", "LabelName", List.of("prop"), true, Optional.of(RANGE))),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE UNIQUE
                    """,
                        new NodeUniqueness(null, "LabelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE c.prop IS NODE UNIQUE
                    """,
                        new NodeUniqueness("testing", "LabelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS UNIQUE
                    """,
                        new NodeUniqueness("testing", "LabelName", List.of("prop"), true, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR (c:LabelName)
                    REQUIRE (c.prop1, c.prop2) IS NODE UNIQUE
                    """,
                        new NodeUniqueness("testing", "LabelName", List.of("prop1", "prop2"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR (c:LabelName)
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new NodeUniqueness("testing", "LabelName", List.of("prop"), true, Optional.of(RANGE))),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new RelationshipExistence(null, "RelName", "prop", false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new RelationshipExistence("testing", "RelName", "prop", false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS NOT NULL
                    """,
                        new RelationshipExistence("testing", "RelName", "prop", true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: STRING
                    """,
                        new RelationshipPropertyType(
                                null, "RelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: STRING
                    """,
                        new RelationshipPropertyType(
                                "testing", "RelName", "prop", PropertyTypeSet.of(SchemaValueType.STRING), false)),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS :: INTEGER
                    """,
                        new RelationshipPropertyType(
                                "testing", "RelName", "prop", PropertyTypeSet.of(SchemaValueType.INTEGER), true)),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP KEY
                    """,
                        new RelationshipKey(null, "RelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP KEY
                    """,
                        new RelationshipKey("testing", "RelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS REL KEY
                    """,
                        new RelationshipKey("testing", "RelName", List.of("prop"), true, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE (c.prop1, c.prop2) IS REL KEY
                    """,
                        new RelationshipKey("testing", "RelName", List.of("prop1", "prop2"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS REL KEY
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new RelationshipKey("testing", "RelName", List.of("prop"), true, Optional.of(RANGE))),
                arguments(
                        """
                    CREATE CONSTRAINT
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP UNIQUE
                    """,
                        new RelationshipUniqueness(null, "RelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS RELATIONSHIP UNIQUE
                    """,
                        new RelationshipUniqueness("testing", "RelName", List.of("prop"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE
                    """,
                        new RelationshipUniqueness("testing", "RelName", List.of("prop"), true, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE (c.prop1, c.prop2) IS REL UNIQUE
                    """,
                        new RelationshipUniqueness(
                                "testing", "RelName", List.of("prop1", "prop2"), false, Optional.empty())),
                arguments(
                        """
                    CREATE CONSTRAINT testing IF NOT EXISTS
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE
                    OPTIONS { indexProvider: 'range-1.0' }
                    """,
                        new RelationshipUniqueness("testing", "RelName", List.of("prop"), true, Optional.of(RANGE))),

                // MULTIPLES
                Arguments.of(
                        """
                    DROP INDEX testing1;
                    DROP CONSTRAINT testing2;
                    """,
                        List.of(
                                new IndexCommand.Drop("testing1", false),
                                new ConstraintCommand.Drop("testing2", false))),
                Arguments.of(
                        """
                    DROP INDEX testing1;
                    DROP INDEX testing1;
                    """,
                        List.of(new IndexCommand.Drop("testing1", false))),
                Arguments.of(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    DROP INDEX testing;
                    """,
                        List.of()),
                Arguments.of(
                        """
                    CYPHER 5
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    """,
                        List.of(new NodeRange(
                                "testing", "LabelName", List.of("propertyName"), false, Optional.empty()))),
                Arguments.of(
                        """
                    DROP INDEX testing;
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName);
                    """,
                        List.of(
                                new IndexCommand.Drop("testing", false),
                                new NodeRange(
                                        "testing", "LabelName", List.of("propertyName"), false, Optional.empty()))),
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
                                new NodeRange(
                                        "testing1", "LabelName", List.of("propertyName"), false, Optional.empty()),
                                new NodeText("testing2", "LabelName", "propertyName", false, Optional.empty()))),
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
                                new NodeUniqueness(
                                        "testing1", "LabelName", List.of("propString"), false, Optional.empty()),
                                new NodeRange("testing2", "LabelName", List.of("propInt"), false, Optional.empty()))));
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
                arguments(
                        """
                    CREATE INDEX testing1
                    FOR (n:LabelName1)
                    ON (n.propertyName);
                    CREATE INDEX testing2
                    FOR (n:LabelName2)
                    ON (n.propertyName);
                    """,
                        "Variable `n` already declared"),
                // drop
                arguments("DROP CONSTRAINT $testing", "Parameters are not allowed to be used as a constraint name"),
                arguments(
                        "DROP INDEX boom DROP CONSTRAINT testing2",
                        "Invalid input 'DROP': expected 'IF EXISTS' or <EOF>"),
                // invalid options
                arguments(
                        """
                    CREATE TEXT INDEX testing
                    FOR (n:LabelName1)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'vector-1.0' }
                    """,
                        "The provider 'vector-1.0' of type VECTOR does not match the expected type of TEXT"),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName1)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'point-1.0' }
                    """,
                        "The provider 'point-1.0' of type POINT does not match the expected type of RANGE"),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { provider: 'duff' }
                    """,
                        "Unable to parse the Cypher",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: ['duff'] }
                    """,
                        "Could not create range node property index with specified index provider 'List{String(\"duff\")}'",
                        "Expected String value"),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: 'duff' }
                    """,
                        "Unable to parse the Cypher",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { config: {go:['boom']} }
                    """,
                        "Unable to parse the Cypher",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName)
                    OPTIONS { indexProvider: 'range-1.0', config: ['boom'] }
                    """,
                        "Unable to parse the Cypher",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (n.propertyName, n.propertyName)
                    """,
                        "Invalid range node property index as property 'propertyName' is duplicated"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.propertyName, r.propertyName)
                    """,
                        "Invalid range relationship property index as property 'propertyName' is duplicated"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR (n:LabelName)
                    ON (x.propertyName)
                    """,
                        "Unable to parse the Cypher",
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (x.propertyName)
                    """,
                        "Unable to parse the Cypher",
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE TEXT INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """,
                        "Unable to parse the Cypher",
                        "Only single property text indexes are supported"),
                arguments(
                        """
                    CREATE TEXT INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """,
                        "Only single property text indexes are supported"),
                arguments(
                        """
                    CREATE POINT INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    """,
                        "Unable to parse the Cypher",
                        "Only single property point indexes are supported"),
                arguments(
                        """
                    CREATE POINT INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    """,
                        "Only single property point indexes are supported"),
                arguments(
                        """
                    CREATE VECTOR INDEX boom
                    FOR (n:LabelName)
                    ON (n.property1, n.property2)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      }
                    }
                    """,
                        "Unable to parse the Cypher",
                        "Only single property vector indexes are supported"),
                arguments(
                        """
                    CREATE VECTOR INDEX boom
                    FOR ()-[r:RelName]-()
                    ON (r.property1, r.property2)
                    OPTIONS {
                      indexConfig: {
                        `vector.dimensions`: 1536,
                        `vector.similarity_function`: 'COSINE'
                      }
                    }
                    """,
                        "Only single property vector indexes are supported"),
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
                arguments(
                        """
                    CREATE LOOKUP INDEX boom
                    FOR (n)
                    ON EACH labels(x)
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE LOOKUP INDEX boom
                    FOR (n)
                    ON EACH foo(n)
                    """,
                        "Function 'foo' is not allowed, valid function is 'labels'"),
                arguments(
                        """
                    CREATE LOOKUP INDEX boom
                    FOR ()-[r]-()
                    ON EACH type(x)
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE FULLTEXT INDEX boom
                    FOR (n:LabelName|LabelName)
                    ON EACH [n.property1, n.property2]
                    """,
                        "Invalid fulltext node index as label 'LabelName' is duplicated"),
                arguments(
                        """
                    CREATE FULLTEXT INDEX boom
                    FOR ()-[r:RelName|RelName]-()
                    ON EACH [r.property1, r.property2]
                    """,
                        "Invalid fulltext relationship index as relationship 'RelName' is duplicated"),
                arguments(
                        """
                    CREATE LOOKUP INDEX boom
                    FOR ()-[r]-()
                    ON EACH foo(n)
                    """,
                        "Function 'foo' is not allowed, valid function is 'type'"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS UNIQUE
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create uniqueness constraint",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS NOT NULL
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE (n.prop1, n.prop2) IS NOT NULL
                    """,
                        "Constraint type 'IS NOT NULL' does not allow multiple properties"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create node property existence constraint",
                        "Invalid option provided"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS NODE KEY
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS NODE KEY
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create node key constraint",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE x.prop IS :: STRING
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE (n.prop1, n.prop2) IS :: STRING
                    """,
                        "Constraint type 'IS TYPED' does not allow multiple properties"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR (n:LabelName)
                    REQUIRE n.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create node property type constraint",
                        "Invalid option provided"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS UNIQUE
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS UNIQUE
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create relationship uniqueness constraint",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS NOT NULL
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE (r.prop1, r.prop2) IS NOT NULL
                    """,
                        "Constraint type 'IS NOT NULL' does not allow multiple properties"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS NOT NULL
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create relationship property existence constraint",
                        "Invalid option provided"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS REL KEY
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS REL KEY
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create relationship key constraint",
                        "Invalid option provided, valid options are `indexProvider` and `indexConfig`"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE x.prop IS :: STRING
                    """,
                        "Variable `x` not defined"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE (r.prop1, r.prop2) IS :: STRING
                    """,
                        "Constraint type 'IS TYPED' does not allow multiple properties"),
                arguments(
                        """
                    CREATE CONSTRAINT boom
                    FOR ()-[r:RelName]-()
                    REQUIRE r.prop IS :: STRING
                    OPTIONS { duff:13 }
                    """,
                        "Failed to create relationship property type constraint",
                        "Invalid option provided"),
                // mixed index/constraint violations
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n1:LabelName1)
                    ON (n1.propertyName);
                    CREATE INDEX testing
                    FOR (n2:LabelName2)
                    ON (n2.propertyName);
                    """,
                        "Multiple operations for the schema command with name testing"),
                arguments(
                        """
                    CREATE INDEX testing
                    FOR (n1:LabelName1)
                    ON (n1.propertyName);
                    CREATE CONSTRAINT testing
                    FOR ()-[c:RelName]-()
                    REQUIRE c.prop IS UNIQUE;
                    """,
                        "Multiple operations for the schema command with name testing"),
                // lookups
                arguments(
                        """
                    CREATE LOOKUP INDEX testing1 FOR (n1) ON EACH labels(n1);
                    CREATE LOOKUP INDEX testing2 FOR (n2) ON EACH labels(n2);
                    """,
                        "Multiple node lookup indexes found - only 1 is allowed per database: testing1,testing2"),
                arguments(
                        """
                    CREATE LOOKUP INDEX testing1 FOR ()-[r1]-() ON EACH type(r1);
                    CREATE LOOKUP INDEX testing2 FOR ()-[r2]-() ON EACH type(r2);
                    """,
                        "Multiple relationship lookup indexes found - only 1 is allowed per database: testing1,testing2"),
                arguments(
                        """
                    CREATE INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propertyName);
                    """,
                        "An index of type 'RANGE' is also specified - unable to create index 'testing2'"),
                arguments(
                        """
                    CREATE POINT INDEX testing1
                    FOR (n1:LabelName)
                    ON (n1.propertyName);
                    CREATE POINT INDEX testing2
                    FOR (n2:LabelName)
                    ON (n2.propertyName);
                    """,
                        "An index of type 'POINT' is also specified - unable to create index 'testing2'"),
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
                arguments(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS UNIQUE;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS UNIQUE;
                    """,
                        "Duplicate backing indexes found for constraints 'testing1' and 'testing2'"),
                arguments(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS NOT NULL;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS NOT NULL;
                    """,
                        "Duplicate schemas found for constraints 'testing1' and 'testing2'"),
                arguments(
                        """
                    CREATE CONSTRAINT testing1
                    FOR (n1:LabelName)
                    REQUIRE n1.propertyName IS :: STRING;
                    CREATE CONSTRAINT testing2
                    FOR (n2:LabelName)
                    REQUIRE n2.propertyName IS :: STRING;
                    """,
                        "Duplicate schemas found for constraints 'testing1' and 'testing2'"));
    }
}
