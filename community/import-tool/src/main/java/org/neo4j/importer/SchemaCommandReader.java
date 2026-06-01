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

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.cypher.internal.schema.SchemaCommandConverter;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

/**
 * Reads a file that contains Cypher schema commands and converts them into the appropriate {@link SchemaCommand}s.
 */
public class SchemaCommandReader {
    private final FileSystemAbstraction fileSystem;
    private final ReaderConfig readerConfig;
    private final SchemaCommandParser parser;
    private final SchemaCommandConverter converter;

    public SchemaCommandReader(
            FileSystemAbstraction fileSystem,
            SchemaCommandParser parser,
            SchemaCommandConverter converter,
            ReaderConfig readerConfig) {
        this.fileSystem = requireNonNull(fileSystem);
        this.readerConfig = requireNonNull(readerConfig);
        this.parser = requireNonNull(parser);
        this.converter = requireNonNull(converter);
    }

    public SchemaCommandReader(
            FileSystemAbstraction fileSystem, SchemaCommandParser parser, ReaderConfig readerConfig) {
        this(fileSystem, parser, new SchemaCommandConverter(readerConfig.config()), readerConfig);
    }

    /**
     * @param cypherPath the {@link Path} to the Cypher statement to parse
     * @return the {@link SchemaCommand} objects representing the Cypher statements at the provided path.
     * @throws SchemaCommandReaderException if unable to parse the Cypher content
     */
    public List<SchemaCommand> parse(Path cypherPath) throws SchemaCommandReaderException, IOException {
        Preconditions.checkState(
                cypherPath != null && fileSystem.fileExists(cypherPath),
                "The path to the Cypher schema commands must exist");

        String cypherText = FileSystemUtils.readString(fileSystem, cypherPath, EmptyMemoryTracker.INSTANCE);
        if (cypherText == null || cypherText.isEmpty()) {
            return List.of();
        }
        return parse(cypherText);
    }

    public List<SchemaCommand> parse(String cypherText) {
        switch (parser.parse(cypherText)) {
            case ParseResult.Success(
                    CypherVersion version,
                    List<org.neo4j.cypher.internal.ast.SchemaCommand> statements) -> {
                final var changesBuilder = new SchemaCommandsBuilder(readerConfig, converter);
                for (var statement : statements) {
                    changesBuilder.withCommand(statement, version);
                }
                return changesBuilder.build();
            }
            case ParseResult.Failure(List<String> errors) -> {
                var sb = new StringBuilder();
                sb.append("Unable to parse the Cypher in import change commands.");
                errors.forEach(e -> sb.append(System.lineSeparator()).append(e));
                throw new SchemaCommandReaderException(sb.toString());
            }
        }
    }

    public sealed interface ReaderConfig {
        boolean allowEnterpriseFeatures();

        boolean allowDropOperations();

        Config config();

        VectorIndexVersion latestVectorIndexVersion();

        static ReaderConfig communityImporter(Config config) {
            return new ReaderConfigImpl(false, false, config);
        }

        static ReaderConfig enterpriseImporter(Config config) {
            return new ReaderConfigImpl(true, false, config);
        }

        static ReaderConfig enterpriseIncrementalImporter(Config config) {
            return new ReaderConfigImpl(true, true, config);
        }

        @VisibleForTesting
        static ReaderConfig forTesting(boolean allowEnterpriseFeatures, boolean allowDropOperations, Config config) {
            return new ReaderConfigImpl(allowEnterpriseFeatures, allowDropOperations, config);
        }
    }

    private record ReaderConfigImpl(
            boolean allowEnterpriseFeatures,
            boolean allowDropOperations,
            Config config,
            VectorIndexVersion latestVectorIndexVersion)
            implements ReaderConfig {
        ReaderConfigImpl(boolean allowEnterpriseFeatures, boolean allowDropOperations, Config config) {
            this(
                    allowEnterpriseFeatures,
                    allowDropOperations,
                    config,
                    VectorIndexVersion.latestSupportedVersion(KernelVersion.getLatestVersion(config)));
        }
    }
}
