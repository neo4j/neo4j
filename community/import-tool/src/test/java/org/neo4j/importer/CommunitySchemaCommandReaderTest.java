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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.cypher.internal.config.CypherConfiguration;
import org.neo4j.io.fs.FileSystemAbstraction;

public class CommunitySchemaCommandReaderTest extends SchemaCommandReaderTest {
    public SchemaCommandReader createReader(FileSystemAbstraction fs, SchemaCommandReader.ReaderConfig readerConfig) {
        return new SchemaCommandReader(
                fs,
                SchemaCommandParser.createCommunity(CypherConfiguration.fromConfig(readerConfig.config())),
                readerConfig);
    }

    @Test
    void doesNotParseGraphTypeStatements() throws IOException {
        var cypher = createCypher("CYPHER 25 ALTER CURRENT GRAPH TYPE SET { }");
        var reader = createReader(SchemaCommandReader.ReaderConfig.forTesting(true, true, CONFIG));
        assertThatThrownBy(() -> reader.parse(cypher))
                .hasMessageContainingAll("GRAPH TYPE requires Enterprise Edition");
    }
}
