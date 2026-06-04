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

import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCommand;
import org.neo4j.token.TokenHolders;

public sealed interface SchemaCommandSource {

    record ResolvedSchemaCommands(List<SchemaCommand> commands) implements SchemaCommandSource {

        private static final ResolvedSchemaCommands NO_COMMANDS = new ResolvedSchemaCommands(List.of());

        public static ResolvedSchemaCommands of(List<SchemaCommand> commands) {
            if (commands.isEmpty()) {
                return NO_COMMANDS;
            }
            return new ResolvedSchemaCommands(commands);
        }

        public static ResolvedSchemaCommands of(SchemaCommand... commands) {
            if (commands.length == 0) {
                return NO_COMMANDS;
            }
            return new ResolvedSchemaCommands(List.of(commands));
        }
    }

    record DeferredSchemaCommands(BiFunction<Adaptor, TokenHolders, List<SchemaCommand>> deferred)
            implements SchemaCommandSource {
        public ResolvedSchemaCommands resolve(Adaptor adaptor, TokenHolders tokenHolders) {
            return ResolvedSchemaCommands.of(deferred.apply(adaptor, tokenHolders));
        }

        public interface Adaptor {
            Iterator<ConstraintDescriptor> getAllConstraints();

            IndexDescriptor getIndexForName(String name);
        }
    }

    static boolean mayHaveCommands(SchemaCommandSource src) {
        return switch (src) {
            case DeferredSchemaCommands ignored -> true; // We don't know, so possibly yes.
            case ResolvedSchemaCommands resolved -> !resolved.commands().isEmpty();
        };
    }
}
