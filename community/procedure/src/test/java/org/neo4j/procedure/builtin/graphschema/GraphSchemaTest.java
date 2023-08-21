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
package org.neo4j.procedure.builtin.graphschema;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.ReflectionUtils;

class GraphSchemaTest {

    @Nested
    class IntrospectTest {

        @Test
        void shouldSampleByDefault() throws InvocationTargetException, IllegalAccessException {

            var getRelationshipPropertiesQuery = ReflectionUtils.getRequiredMethod(
                    GraphSchema.Introspector.class, "getRelationshipPropertiesQuery", Introspect.Config.class);
            getRelationshipPropertiesQuery.setAccessible(true);
            var query = getRelationshipPropertiesQuery.invoke(null, new Introspect.Config(Map.of()));
            assertThat(query)
                    .isEqualTo(
                            """
				CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory
				WITH substring(relType, 2, size(relType)-3) AS relType, propertyName, propertyTypes, mandatory
				CALL {
					WITH relType, propertyName
					MATCH (n)-[r]->(m) WHERE type(r) = relType AND (r[propertyName] IS NOT NULL OR propertyName IS NULL)
					WITH n, r, m
					LIMIT 100
					WITH DISTINCT labels(n) AS from, labels(m) AS to
					RETURN from, to
				}
				RETURN DISTINCT from, to, relType, propertyName, propertyTypes, mandatory
				ORDER BY relType ASC
				""");
        }

        @Test
        void sampleCanBeDisabled() throws InvocationTargetException, IllegalAccessException {

            var getRelationshipPropertiesQuery = ReflectionUtils.getRequiredMethod(
                    GraphSchema.Introspector.class, "getRelationshipPropertiesQuery", Introspect.Config.class);
            getRelationshipPropertiesQuery.setAccessible(true);
            var query = getRelationshipPropertiesQuery.invoke(null, new Introspect.Config(Map.of("sampleOnly", false)));
            assertThat(query)
                    .isEqualTo(
                            """
				CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory
				WITH substring(relType, 2, size(relType)-3) AS relType, propertyName, propertyTypes, mandatory
				CALL {
					WITH relType, propertyName
					MATCH (n)-[r]->(m) WHERE type(r) = relType AND (r[propertyName] IS NOT NULL OR propertyName IS NULL)
					WITH n, r, m
					// LIMIT
					WITH DISTINCT labels(n) AS from, labels(m) AS to
					RETURN from, to
				}
				RETURN DISTINCT from, to, relType, propertyName, propertyTypes, mandatory
				ORDER BY relType ASC
				""");
        }
    }
}
