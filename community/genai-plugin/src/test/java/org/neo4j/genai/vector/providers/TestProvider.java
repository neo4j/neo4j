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
package org.neo4j.genai.vector.providers;

import java.util.Map;
import java.util.Optional;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.genai.vector.VectorEncoding.Provider;
import org.neo4j.values.storable.FloatArray;
import org.neo4j.values.storable.Values;

@ServiceProvider
public class TestProvider implements Provider<TestProvider.Parameters> {
    public static final String NAME = "test-provider";
    public static final FloatArray VECTOR = Values.floatArray(new float[] {1.f, 2.f, 3.f});

    public static final String REQUIRED_CONFIG_TYPE = "{  }";
    public static final String OPTIONAL_CONFIG_TYPE = "{ model :: STRING NOT NULL, dimensions :: INTEGER }";
    public static final Map<String, Object> DEFAULT_CONFIG = Map.of("model", "testModel");

    public static class Parameters {
        public String model = "testModel";
        public Optional<Long> dimensions;
    }

    @Override
    public Class<Parameters> parameterDeclarations() {
        return Parameters.class;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Encoder configure(Parameters configuration) {
        return text -> VECTOR.asObjectCopy();
    }
}
