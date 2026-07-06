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
package org.neo4j.genai.ai.text.tokenChunking;

import static java.util.Objects.requireNonNull;

import com.knuddels.jtokkit.api.Encoding;
import java.util.List;
import org.neo4j.genai.GenAIConfig;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.procedure.QueryLanguageScope;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

public class TextChunkByToken {

    @Context
    public GenAIConfig genAIConfig;

    @UserFunction(name = "ai.text.chunkByTokenLimit")
    @QueryLanguageScope(scope = {QueryLanguage.CYPHER_25})
    @Description("Chunk the given string into a list of string chunks based on the token limit.")
    public List<String> chunkByLimit(
            @Name(value = "input", description = "The input to chunk.") String prompt,
            @Name(value = "limit", description = "The maximum token limit of a single chunk.") Long limit,
            @Name(value = "model", description = "The OpenAI model to chunk by.", defaultValue = "ada") String model,
            @Name(value = "overlap", description = "The amount of tokens to overlap by.", defaultValue = "0")
                    Long overlap) {
        requireNonNull(limit, "'limit' must not be null");
        TextChunkConfig textChunkConfig = new TextChunkConfig(limit, overlap, model);

        if (prompt == null) {
            return null;
        }

        Encoding enc = textChunkConfig.getEncoding();
        RecursiveTokenSplitter splitter = new RecursiveTokenSplitter(enc, limit.intValue(), overlap.intValue());
        return splitter.splitText(prompt);
    }
}
