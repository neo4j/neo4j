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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.configuration.Config.defaults;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.values.storable.ValueType.CHAR;
import static org.neo4j.values.storable.ValueType.STRING;
import static org.neo4j.values.storable.ValueType.STRING_ALPHANUMERIC;
import static org.neo4j.values.storable.ValueType.STRING_ASCII;
import static org.neo4j.values.storable.ValueType.STRING_BMP;

import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.api.impl.schema.TextIndexProvider;
import org.neo4j.monitoring.Monitors;

class TextIndexPopulationStressTest extends IndexPopulationStressTest {
    TextIndexPopulationStressTest() {
        super(
                true,
                randomValues ->
                        randomValues.nextValueOfTypes(CHAR, STRING, STRING_ALPHANUMERIC, STRING_ASCII, STRING_BMP),
                test -> {
                    DatabaseIndexContext context = DatabaseIndexContext.builder(
                                    test.pageCache,
                                    test.fs,
                                    test.contextFactory,
                                    test.pageCacheTracer,
                                    DEFAULT_DATABASE_NAME)
                            .build();
                    return new TextIndexProvider(
                            context.fileSystem,
                            directoryFactory(context.fileSystem),
                            test.directory(),
                            new Monitors(),
                            defaults(),
                            writable());
                });
    }

    @Override
    IndexType indexType() {
        return IndexType.TEXT;
    }
}
