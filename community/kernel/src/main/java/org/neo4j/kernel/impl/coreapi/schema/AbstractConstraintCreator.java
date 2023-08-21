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
package org.neo4j.kernel.impl.coreapi.schema;

import java.util.Arrays;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.graphdb.schema.PropertyType;
import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.internal.schema.constraints.SchemaValueType;
import org.neo4j.internal.schema.constraints.TypeRepresentation;

abstract class AbstractConstraintCreator {
    protected final InternalSchemaActions actions;
    protected final String name;
    protected final IndexType indexType;
    protected final IndexConfig indexConfig;

    AbstractConstraintCreator(
            InternalSchemaActions actions, String name, IndexType indexType, IndexConfig indexConfig) {
        this.actions = actions;
        this.name = name;
        this.indexType = indexType;
        this.indexConfig = indexConfig;
    }

    public ConstraintDefinition create() {
        assertInUnterminatedTransaction();
        throw new IllegalStateException("No constraint assertions specified");
    }

    protected final void assertInUnterminatedTransaction() {
        actions.assertInOpenTransaction();
    }

    protected PropertyTypeSet validatePropertyTypes(PropertyType[] propertyType) {
        var internalTypes =
                Arrays.stream(propertyType).map(SchemaValueType::fromPublicApi).toList();
        var out = PropertyTypeSet.of(internalTypes);
        TypeRepresentation.validate(out);
        return out;
    }
}
