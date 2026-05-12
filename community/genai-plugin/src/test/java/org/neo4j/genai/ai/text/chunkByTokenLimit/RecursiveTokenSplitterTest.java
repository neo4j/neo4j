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
package org.neo4j.genai.ai.text.chunkByTokenLimit;

import static org.assertj.core.api.Assertions.assertThat;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.neo4j.genai.ai.text.tokenChunking.RecursiveTokenSplitter;

class RecursiveTokenSplitterTest {

    private Encoding encoding;
    private RecursiveTokenSplitter splitter;

    @BeforeEach
    void setUp() {
        EncodingRegistry registry = Encodings.newDefaultEncodingRegistry();
        encoding = registry.getEncoding(EncodingType.CL100K_BASE);
    }

    @Test
    @DisplayName("Should return a single chunk if text is smaller than chunk size")
    void testShortText() {
        splitter = new RecursiveTokenSplitter(encoding, 100, 10);
        String text = "This is a short sentence.";

        List<String> chunks = splitter.splitText(text);

        assertThat(chunks).hasSize(1);
        assertThat(chunks).first().isEqualTo(text);
    }

    @Test
    @DisplayName("Should split by paragraphs when available")
    void testParagraphSplitting() {
        // Create two paragraphs, each roughly 10-15 tokens
        String p1 = "This is the first paragraph. It is relatively short.";
        String p2 = "This is the second paragraph. It follows a double newline.";
        String text = p1 + "\n\n" + p2;

        // Set chunk size small enough to force a split at the paragraph level
        splitter = new RecursiveTokenSplitter(encoding, 15, 0);

        List<String> chunks = splitter.splitText(text);

        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0)).contains("first paragraph");
        assertThat(chunks.get(1)).contains("second paragraph");
    }

    @Test
    @DisplayName("Should maintain overlap between chunks")
    void testOverlapLogic() {
        splitter = new RecursiveTokenSplitter(encoding, 20, 10);
        String text = "Word1 Word2 Word3 Word4 Word5 Word6 Word7 Word8 Word9 Word10";

        List<String> chunks = splitter.splitText(text);

        assertThat(chunks).hasSizeGreaterThanOrEqualTo(1);

        // If it split, check overlap
        if (chunks.size() > 1) {
            assertThat(chunks.get(0)).contains("Word5");
            assertThat(chunks.get(1)).contains("Word5");
        }
    }

    @Test
    @DisplayName("Should fallback to word splitting if no newlines exist")
    void testNoNewlinesFallback() {
        splitter = new RecursiveTokenSplitter(encoding, 5, 0);
        String text = "One Two Three Four Five Six Seven Eight Nine Ten";

        List<String> chunks = splitter.splitText(text);

        assertThat(chunks).hasSizeGreaterThan(1);
        // Ensure no chunk exceeds the limit
        for (String chunk : chunks) {
            assertThat(encoding.countTokens(chunk.trim())).isLessThanOrEqualTo(5);
        }
    }

    @Test
    @DisplayName("Should handle 'unbreakable' text (hard cut) as a last resort")
    void testHardCutFallback() {
        // A massive string with no spaces or newlines
        String unbreakable = "a".repeat(100);
        splitter = new RecursiveTokenSplitter(encoding, 10, 0);

        List<String> chunks = splitter.splitText(unbreakable);

        assertThat(chunks).hasSizeGreaterThan(1);
        assertThat(encoding.countTokens(chunks.getFirst())).isEqualTo(10);
        assertThat(String.join("", chunks)).isEqualTo(unbreakable);
    }

    @Test
    @DisplayName("Should handle 'unbreakable' text (hard cut) as a last resort with chunkoverlap")
    void testHardCutFallbackWithChunkOverlap() {
        // A massive string with no spaces or newlines
        String unbreakable = "a".repeat(100);
        splitter = new RecursiveTokenSplitter(encoding, 10, 5);

        List<String> chunks = splitter.splitText(unbreakable);

        assertThat(chunks).hasSizeGreaterThan(1);
        assertThat(encoding.countTokens(chunks.getFirst())).isEqualTo(10);
        assertThat(String.join("", chunks).length()).isGreaterThan(100);
    }

    @Test
    @DisplayName("Should handle empty string input gracefully")
    void testEmptyInput() {
        splitter = new RecursiveTokenSplitter(encoding, 100, 10);
        List<String> chunks = splitter.splitText("");

        assertThat(chunks).hasSize(1);
        assertThat(chunks.getFirst()).isEmpty();
    }

    @Test
    @DisplayName("Should preserve trailing separator at the end of text")
    void testPreserveTrailingSeparator() {
        // Space is a separator. If text ends with " ", it should be kept.
        splitter = new RecursiveTokenSplitter(encoding, 5, 0);
        String text = "word1 word2 ";
        List<String> chunks = splitter.splitText(text);

        // All chunks joined should equal original text
        assertThat(String.join("", chunks)).isEqualTo(text);
    }

    @Test
    @DisplayName("Should preserve trailing newline at the end of text")
    void testPreserveTrailingNewline() {
        // Newline is a separator. If text ends with "\n", it should be kept.
        splitter = new RecursiveTokenSplitter(encoding, 10, 0);
        String text = "word1 word2\n";
        List<String> chunks = splitter.splitText(text);

        // All chunks joined should equal original text
        assertThat(String.join("", chunks)).isEqualTo(text);
    }

    @Test
    @DisplayName("Should preserve trailing paragraph separator at the end of text")
    void testPreserveTrailingParagraph() {
        splitter = new RecursiveTokenSplitter(encoding, 10, 0);
        String text = "word1 word2\n\n";
        List<String> chunks = splitter.splitText(text);

        assertThat(String.join("", chunks)).isEqualTo(text);
    }

    @Test
    @DisplayName("Should preserve trailing space when it's the last token and exceeds chunk size")
    void testPreserveTrailingSpaceForceSplit() {
        // limit 1, word1 (1 token), space (1 token), word2 (1 token)
        splitter = new RecursiveTokenSplitter(encoding, 1, 0);
        String text = "a b ";
        List<String> chunks = splitter.splitText(text);

        assertThat(String.join("", chunks)).isEqualTo(text);
    }
}
