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
package org.neo4j.genai.util.provider;

import static java.lang.String.CASE_INSENSITIVE_ORDER;

import java.util.Arrays;
import java.util.List;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.neo4j.genai.ai.image.embed.ImageVectorEmbedding;
import org.neo4j.genai.ai.text.chat.TextChat;
import org.neo4j.genai.ai.text.completion.TextCompletion;
import org.neo4j.genai.ai.text.embed.VectorEmbedding;
import org.neo4j.genai.ai.text.structuredCompletion.TextStructuredCompletion;
import org.neo4j.genai.ai.text.tokenCount.TextTokenCount;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.util.Preconditions;

/** Keeps track of AI providers. */
public interface GlobalProviders {
    <T extends NamedProvider> ImmutableList<T> providers(Class<T> type);

    @CalledFromGeneratedCode // Called through reflection from metrics
    default <T extends NamedProvider> List<String> providerMetricsNames(Class<T> type) {
        return providers(type).stream()
                .map(NamedProvider::metricsName)
                .distinct()
                .toList();
    }

    static GlobalProviders from(final NamedProvider... providers) {
        final var providersByType = Lists.immutable.of(providers).groupBy(GlobalProviders::findType);

        providersByType.keysView().forEach(type -> {
            final var typeProviders = providersByType.get(type);

            Preconditions.checkArgument(
                    namesAreUnique(typeProviders.toArray(new NamedProvider[0])),
                    "Provider names are not unique within type: " + type.getSimpleName());
        });
        return new GlobalProviders() {
            @Override
            public <T extends NamedProvider> ImmutableList<T> providers(Class<T> type) {
                return providersByType.get(type).collect(type::cast);
            }
        };
    }

    @SuppressWarnings("rawtypes")
    private static Class findType(NamedProvider provider) {
        if (provider instanceof TextCompletion.Provider) return TextCompletion.Provider.class;
        if (provider instanceof TextStructuredCompletion.Provider) return TextStructuredCompletion.Provider.class;
        if (provider instanceof TextChat.Provider) return TextChat.Provider.class;
        if (provider instanceof TextTokenCount.Provider) return TextTokenCount.Provider.class;
        if (provider instanceof VectorEmbedding.Provider) return VectorEmbedding.Provider.class;
        if (provider instanceof ImageVectorEmbedding.Provider) return ImageVectorEmbedding.Provider.class;
        throw new IllegalArgumentException("Unknown provider type: " + provider.getClass());
    }

    private static boolean namesAreUnique(final NamedProvider... providers) {
        final var names = Arrays.stream(providers).map(NamedProvider::name).toArray(String[]::new);
        Arrays.sort(names, CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < names.length - 1; i++) {
            if (CASE_INSENSITIVE_ORDER.compare(names[i], names[i + 1]) == 0) {
                return false;
            }
        }
        return true;
    }
}
