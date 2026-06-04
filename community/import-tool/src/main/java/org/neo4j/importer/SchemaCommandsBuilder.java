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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.impl.factory.Multimaps;
import org.neo4j.common.EntityType;
import org.neo4j.cypher.internal.CypherVersion;
import org.neo4j.cypher.internal.schema.SchemaCommandConverter;
import org.neo4j.importer.SchemaCommandReader.ReaderConfig;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeLabelExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipEndpointLabel;
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
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;

class SchemaCommandsBuilder {

    private static final String NODE_LOOKUP_KEY = schemaKey(EntityType.NODE, List.of(), List.of());
    private static final String REL_LOOKUP_KEY = schemaKey(EntityType.RELATIONSHIP, List.of(), List.of());

    private final List<SchemaCommand> allCommands = Lists.mutable.empty();

    private final ReaderConfig readerConfig;

    private final SchemaCommandConverter schemaCommandConverter;

    private int numGraphTypeStatements = 0;
    private int numConstraintStatements = 0;

    SchemaCommandsBuilder(ReaderConfig readerConfig, SchemaCommandConverter commandConverter) {
        this.readerConfig = Objects.requireNonNull(readerConfig);
        this.schemaCommandConverter = Objects.requireNonNull(commandConverter);
    }

    List<SchemaCommand> build() {
        return validateParsedCommands();
    }

    @SuppressWarnings("UnusedReturnValue")
    SchemaCommandsBuilder withCommand(org.neo4j.cypher.internal.ast.SchemaCommand ast, CypherVersion cypherVersion)
            throws SchemaCommandReaderException {
        final var command = schemaCommandConverter.apply(ast, cypherVersion, readerConfig.latestVectorIndexVersion());
        if (!readerConfig.allowEnterpriseFeatures()) {
            if (isEnterpriseOnly(command)) {
                throw new SchemaCommandReaderException("Enterprise features are not currently supported");
            }
        } else if (!readerConfig.allowDropOperations()) {
            if (isDropCommand(command)) {
                throw new SchemaCommandReaderException("Dropping indexes or constraints is not currently supported");
            }
        }

        if (command instanceof SchemaCommand.GraphType) {
            numGraphTypeStatements += 1;
        } else if (command instanceof ConstraintCommand) {
            numConstraintStatements += 1;
        }

        allCommands.add(command);
        return this;
    }

    private List<SchemaCommand> validateParsedCommands() throws SchemaCommandReaderException {
        final var result = Lists.mutable.<SchemaCommand>empty();
        final var namedCommands = new LinkedHashMap<String, SchemaCommand>();
        final var allSchemas = Multimaps.mutable.list.<String, SchemaCommand>empty();

        if (numGraphTypeStatements > 0 && numConstraintStatements > 0) {
            throw new SchemaCommandReaderException("Graph type command can not be mixed with constraint commands");
        }

        if (numGraphTypeStatements > 1) {
            throw new SchemaCommandReaderException("Specify a single graph type command");
        }

        for (var command : allCommands) {
            if (command instanceof IndexCommand.Create index) {
                allSchemas.put(schemaKey(index), index);
            } else if (command instanceof ConstraintCommand.Create constraint) {
                allSchemas.put(schemaKey(constraint), constraint);
            }

            final var trackingName = trackingName(command);
            final var previousCommand = namedCommands.computeIfAbsent(trackingName, (key) -> command);
            if (previousCommand == command) {
                // this is the first time we've seen this name
                result.add(command);
                continue;
            }

            if (isDropCommand(command)) {
                if (!isDropCommand(previousCommand)) {
                    // CREATE|DROP would be a no-op so clean up
                    result.remove(previousCommand);
                }

                namedCommands.remove(trackingName);
            } else {
                if (isDropCommand(previousCommand)) {
                    // DROP|CREATE is something we'd need to handle so allow it
                    result.add(command);
                } else {
                    throw new SchemaCommandReaderException(
                            "Multiple operations for the schema command with name " + command.name());
                }
            }
        }

        final var nodeLookups = allSchemas.removeAll(NODE_LOOKUP_KEY);
        if (nodeLookups != null && nodeLookups.size() > 1) {
            throw new SchemaCommandReaderException(
                    "Multiple node lookup indexes found - only 1 is allowed per database: "
                            + nodeLookups.stream()
                                    .map(SchemaCommand::name)
                                    .sorted()
                                    .collect(Collectors.joining(",")));
        }

        final var relLookups = allSchemas.removeAll(REL_LOOKUP_KEY);
        if (relLookups != null && relLookups.size() > 1) {
            throw new SchemaCommandReaderException(
                    "Multiple relationship lookup indexes found - only 1 is allowed per database: "
                            + relLookups.stream()
                                    .map(SchemaCommand::name)
                                    .sorted()
                                    .collect(Collectors.joining(",")));
        }

        allSchemas.forEachKeyMultiValues((key, schemas) -> {
            final var indexCommands = Lists.mutable.<IndexCommand.Create>empty();
            final var constraintCommands = Lists.mutable.<ConstraintCommand.Create>empty();
            for (var schema : schemas) {
                if (schema instanceof IndexCommand.Create command) {
                    indexCommands.add(command);
                } else if (schema instanceof ConstraintCommand.Create command) {
                    constraintCommands.add(command);
                }
            }
            validateRelatedSchemaCommands(indexCommands, constraintCommands);
        });

        return result;
    }

    private static void validateRelatedSchemaCommands(
            RichIterable<IndexCommand.Create> indexCommands, RichIterable<ConstraintCommand.Create> constraintCommands)
            throws SchemaCommandReaderException {
        validatePropertyTypes(constraintCommands);

        final var backedConstraints = new LinkedHashMap<ConstraintCommand.Create, IndexType>();
        final var nonBackedConstraints = Lists.mutable.<ConstraintCommand.Create>empty();
        for (var constraint : constraintCommands) {
            backingIndexType(constraint)
                    .ifPresentOrElse(
                            type -> backedConstraints.put(constraint, type),
                            () -> nonBackedConstraints.add(constraint));
        }

        backedConstraints.forEach((backed1, backingType1) -> backedConstraints.forEach((backed2, backingType2) -> {
            if (backed1 != backed2 && backed1.constraintType() == backed2.constraintType()) {
                throw new SchemaCommandReaderException("Duplicate backing indexes found for constraints '%s' and '%s'"
                        .formatted(backed1.name(), backed2.name()));
            }
        }));

        for (var nonBacked1 : nonBackedConstraints) {
            for (var nonBacked2 : nonBackedConstraints) {
                if (nonBacked1 != nonBacked2 && nonBacked1.constraintType() == nonBacked2.constraintType()) {
                    throw new SchemaCommandReaderException("Duplicate schemas found for constraints '%s' and '%s'"
                            .formatted(nonBacked1.name(), nonBacked2.name()));
                }
            }
        }

        final var indexTypes = Sets.mutable.<IndexType>empty();
        for (var command : indexCommands) {
            final var indexType = command.indexType();
            if (!indexTypes.add(indexType)) {
                throw new SchemaCommandReaderException(
                        "An index of type '%s' is also specified - unable to create index '%s'"
                                .formatted(indexType, command.name()));
            }

            backedConstraints.forEach((constraint, backingType) -> {
                if (backingType == indexType) {
                    throw new SchemaCommandReaderException(
                            "Cannot create index '%s' as it clashes with the constraint '%s' also having a backing index of type '%s'"
                                    .formatted(command.name(), constraint.name(), indexType));
                }
            });
        }
    }

    private static void validatePropertyTypes(RichIterable<ConstraintCommand.Create> constraintCommands)
            throws SchemaCommandReaderException {
        PropertyTypeSet lastProperties = null;
        for (var constraint : constraintCommands
                .partition(c -> c.constraintType().enforcesPropertyType())
                .getSelected()) {
            final var propertyTypes = (constraint instanceof NodePropertyType nodeType)
                    ? nodeType.propertyTypes()
                    : ((RelationshipPropertyType) constraint).propertyTypes();
            if (lastProperties != null && !lastProperties.equals(propertyTypes)) {
                throw new SchemaCommandReaderException(
                        "A property type constraint of '%s' is also specified - unable to create '%s' with type '%s'"
                                .formatted(
                                        lastProperties.userDescription(),
                                        constraint.name(),
                                        propertyTypes.userDescription()));
            }

            lastProperties = propertyTypes;
        }
    }

    private static Optional<IndexType> backingIndexType(ConstraintCommand.Create constraint) {
        if (constraint.hasBackingIndex()) {
            return Optional.of(IndexType.RANGE);
        }
        return Optional.empty();
    }

    private static boolean isDropCommand(SchemaCommand command) {
        return command instanceof ConstraintCommand.Drop || command instanceof IndexCommand.Drop;
    }

    private static boolean isEnterpriseOnly(SchemaCommand command) {
        if (command instanceof ConstraintCommand.Create constraint) {
            // only UNIQUE constraints are supported in Community
            return constraint.constraintType() != ConstraintType.UNIQUE;
        }

        return command instanceof SchemaCommand.GraphType;
    }

    /**
     * As a {@link SchemaCommand} may not have an actual name, we generate one here to ensure a unique name for the
     * tracking map
     */
    private static String trackingName(SchemaCommand command) {
        final var name = command.name();
        return name == null ? command.toString() : name;
    }

    private static List<String> allProperties(NodeVector command) {
        final var all = Lists.mutable.<String>empty();
        all.add(command.property());
        all.addAll(command.additionalProperties());
        return all;
    }

    private static List<String> allProperties(RelationshipVector command) {
        final var all = Lists.mutable.<String>empty();
        all.add(command.property());
        all.addAll(command.additionalProperties());
        return all;
    }

    private static String schemaKey(IndexCommand.Create indexCommand) {
        return switch (indexCommand) {
            case NodeLookup ignored -> NODE_LOOKUP_KEY;
            case RelationshipLookup ignored -> REL_LOOKUP_KEY;
            case NodeRange command -> schemaKey(EntityType.NODE, command.label(), command.properties());
            case RelationshipRange command -> schemaKey(EntityType.RELATIONSHIP, command.type(), command.properties());
            case NodeText command -> schemaKey(EntityType.NODE, command.label(), command.property());
            case RelationshipText command -> schemaKey(EntityType.RELATIONSHIP, command.type(), command.property());
            case NodePoint command -> schemaKey(EntityType.NODE, command.label(), command.property());
            case RelationshipPoint command -> schemaKey(EntityType.RELATIONSHIP, command.type(), command.property());
            case NodeFulltext command -> schemaKey(EntityType.NODE, command.labels(), command.properties());
            case RelationshipFulltext command ->
                schemaKey(EntityType.RELATIONSHIP, command.types(), command.properties());
            case NodeVector command -> schemaKey(EntityType.NODE, command.labels(), allProperties(command));
            case RelationshipVector command ->
                schemaKey(EntityType.RELATIONSHIP, command.types(), allProperties(command));
        };
    }

    private static String schemaKey(ConstraintCommand.Create constraint) {
        return switch (constraint) {
            case NodeUniqueness command -> schemaKey(EntityType.NODE, command.label(), command.properties());
            case RelationshipUniqueness command ->
                schemaKey(EntityType.RELATIONSHIP, command.type(), command.properties());
            case NodeKey command -> schemaKey(EntityType.NODE, command.label(), command.properties());
            case RelationshipKey command -> schemaKey(EntityType.RELATIONSHIP, command.type(), command.properties());
            case NodeExistence command -> schemaKey(EntityType.NODE, command.label(), command.property());
            case RelationshipExistence command ->
                schemaKey(EntityType.RELATIONSHIP, command.type(), command.property());
            case NodePropertyType command -> schemaKey(EntityType.NODE, command.label(), command.property());
            case RelationshipPropertyType command ->
                schemaKey(EntityType.RELATIONSHIP, command.type(), command.property());
            case NodeLabelExistence command ->
                schemaKey(EntityType.RELATIONSHIP, command.label(), List.of(":" + command.requiredLabel()));
            case RelationshipEndpointLabel command ->
                schemaKey(
                        EntityType.NODE,
                        command.type(),
                        List.of(
                                ":" + command.requiredLabel(),
                                "@" + command.endpointType().name()));
        };
    }

    private static String schemaKey(EntityType entityType, List<String> entities, List<String> properties) {
        return schemaKey(entityType, String.join(",", entities), properties);
    }

    private static String schemaKey(EntityType entityType, String entities, List<String> properties) {
        return schemaKey(entityType, entities, String.join(",", properties));
    }

    private static String schemaKey(EntityType entityType, String entities, String properties) {
        return "%s|%s|%s".formatted(entityType == EntityType.NODE ? "n" : "r", entities, properties);
    }
}
