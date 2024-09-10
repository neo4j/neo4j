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
import org.neo4j.cypher.internal.PreParser;
import org.neo4j.cypher.internal.ast.Statement;
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext;
import org.neo4j.cypher.internal.ast.semantics.SemanticState;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.cypher.internal.parser.AstParserFactory$;
import org.neo4j.cypher.internal.util.InternalNotificationLogger;
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory;
import org.neo4j.cypher.internal.util.devNullLogger$;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.util.Preconditions;
import scala.Option;

/**
 * Reads a file that contains Cypher schema commands and converts them into the appropriate {@link SchemaCommand}s.
 */
public class SchemaCommandReader {

    private static final String ERROR_SUFFIX = " in import change commands.";

    private static final InternalNotificationLogger NOTIFICATION_LOGGER = devNullLogger$.MODULE$;

    private final FileSystemAbstraction fileSystem;

    private final ReaderConfig readerConfig;

    private final PreParser preParser;

    public SchemaCommandReader(FileSystemAbstraction fileSystem, Config config, ReaderConfig readerConfig) {
        this.fileSystem = requireNonNull(fileSystem);
        this.preParser = new PreParser(CypherConfiguration.fromConfig(requireNonNull(config)));
        this.readerConfig = requireNonNull(readerConfig);
    }

    /**
     * @param cypherPath the {@link Path} to the Cypher statement to parse
     * @return the {@link SchemaCommand} objects representing the Cypher statements at the provided path.
     * @throws SchemaCommandReaderException if unable to parse the Cypher content
     */
    public List<SchemaCommand> parse(Path cypherPath) throws SchemaCommandReaderException {
        final var cypherText = parseFile(cypherPath);
        if (cypherText.isEmpty()) {
            return List.of();
        }

        var semanticState = SemanticState.clean();

        final var preParsedQuery = preParser.preParse(cypherText, NOTIFICATION_LOGGER);
        final var cypherVersion =
                preParsedQuery.options().queryOptions().cypherVersion().actualVersion();

        final var exceptionFactory = new Neo4jCypherExceptionFactory(
                cypherText, Option.apply(preParsedQuery.options().offset()));

        final var statements = AstParserFactory$.MODULE$
                .apply(cypherVersion)
                .apply(preParsedQuery.statement(), exceptionFactory, Option.apply(NOTIFICATION_LOGGER))
                .statements();

        final var changesBuilder = new SchemaCommandsBuilder(readerConfig, cypherVersion);

        final var checkContext = SemanticCheckContext.empty();
        for (int i = 0, length = statements.size(); i < length; i++) {
            semanticState = transform(changesBuilder, statements.get(i), semanticState, checkContext);
        }
        return changesBuilder.build();
    }

    private String parseFile(Path cypherPath) throws SchemaCommandReaderException {
        Preconditions.checkState(
                cypherPath != null && fileSystem.fileExists(cypherPath),
                "The path to the Cypher schema commands must exist");
        try {
            return FileSystemUtils.readString(fileSystem, cypherPath, EmptyMemoryTracker.INSTANCE);
        } catch (IOException ex) {
            throw new SchemaCommandReaderException("Unable to read Cypher statement(s) in " + cypherPath, ex);
        }
    }

    private SemanticState transform(
            SchemaCommandsBuilder changesBuilder,
            Statement statement,
            SemanticState semanticState,
            SemanticCheckContext checkContext)
            throws SchemaCommandReaderException {
        if (statement instanceof org.neo4j.cypher.internal.ast.SchemaCommand command) {
            if (!command.useGraph().isEmpty()) {
                throw new SchemaCommandReaderException("Schema commands are only applied to the database to be imported"
                        + " into so graph names are not allowed: "
                        + command.useGraph().get().graphReference().print());
            }

            final var nextState = checkStatement(statement, semanticState, checkContext);
            changesBuilder.withCommand(command);
            return nextState;
        } else {
            throw new SchemaCommandReaderException("Only schema change clauses are allowed here but found: "
                    + statement.getClass().getSimpleName());
        }
    }

    private static SemanticState checkStatement(
            Statement statement, SemanticState semanticState, SemanticCheckContext checkContext)
            throws SchemaCommandReaderException {
        final var checkResult = statement.semanticCheck().run(semanticState, checkContext);
        if (!checkResult.errors().isEmpty()) {
            final var errorText = new StringBuilder("Unable to parse the Cypher").append(ERROR_SUFFIX);
            checkResult.errors().foreach(error -> {
                errorText.append(System.lineSeparator()).append(error.msg());
                return null;
            });

            throw new SchemaCommandReaderException(errorText.toString());
        }

        return checkResult.state();
    }

    public record ReaderConfig(
            boolean allowEnterpriseFeatures,
            boolean allowConstraints,
            boolean allowDropOperations,
            VectorIndexVersion latestVectorIndexVersion) {
        public static ReaderConfig defaults() {
            // initial implementation will be just for CREATE INDEX commands
            return new ReaderConfig(
                    false,
                    false,
                    false,
                    VectorIndexVersion.latestSupportedVersion(KernelVersion.getLatestVersion(Config.defaults())));
        }
    }
}
