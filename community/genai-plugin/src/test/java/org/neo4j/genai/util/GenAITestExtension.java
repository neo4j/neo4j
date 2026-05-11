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
package org.neo4j.genai.util;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.MapAssert;
import org.neo4j.genai.ai.file.embed.FileVectorEmbedding;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbedding;
import org.neo4j.genai.ai.text.aggregateCompletion.TextAggregateCompletion;
import org.neo4j.genai.ai.text.aggregateStructuredCompletion.TextAggregateStructuredCompletion;
import org.neo4j.genai.ai.text.chat.TextChat;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.ai.text.tokenChunking.TextChunkByToken;
import org.neo4j.genai.ai.text.tokenCount.TextTokenCount;
import org.neo4j.genai.vector.DeprecatedVectorEncoding;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.test.jar.JarBuilder;
import org.neo4j.test.utils.TestDirectory;

public interface GenAITestExtension {

    default void installPlugin(TestDirectory testDirectory) throws IOException {
        final var path = testDirectory.directory("plugins").resolve("genai.jar");
        Files.createDirectories(path.getParent());
        JarBuilder.createJarFor(
                path,
                DeprecatedVectorEncoding.class,
                TextCompletion.class,
                TextStructuredCompletion.class,
                TextAggregateCompletion.class,
                TextAggregateStructuredCompletion.class,
                TextTokenCount.class,
                TextChunkByToken.class,
                TextChat.class,
                VectorEmbedding.class,
                FileVectorEmbedding.class,
                ImageVectorEmbedding.class);
    }

    default ResultTransformer<List<Map<String, Object>>> consume() {
        return result -> result.stream().toList();
    }

    default InstanceOfAssertFactory<Map, MapAssert<String, Object>> resultMap() {
        return InstanceOfAssertFactories.map(String.class, Object.class);
    }
}
