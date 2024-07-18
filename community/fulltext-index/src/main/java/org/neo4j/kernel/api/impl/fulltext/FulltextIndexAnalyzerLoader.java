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
package org.neo4j.kernel.api.impl.fulltext;

import static java.lang.String.format;
import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.ANALYZER;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.analysis.Analyzer;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.graphdb.schema.AnalyzerProvider;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.service.Services;
import org.neo4j.values.storable.TextValue;

public final class FulltextIndexAnalyzerLoader {
    public static final FulltextIndexAnalyzerLoader INSTANCE = new FulltextIndexAnalyzerLoader();

    private final ConcurrentHashMap<String, AnalyzerProvider> analyzerProviders = new ConcurrentHashMap<>();

    private FulltextIndexAnalyzerLoader() {}

    public Analyzer createAnalyzer(IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        TextValue analyzerName = descriptor.getIndexConfig().get(ANALYZER);
        if (analyzerName == null) {
            throw new RuntimeException(
                    "Index has no analyzer configured: " + descriptor.userDescription(tokenNameLookup));
        }
        return createAnalyzerFromString(analyzerName.stringValue());
    }

    public Analyzer createAnalyzerFromString(String analyzerName) {
        AnalyzerProvider provider = analyzerProviders.get(analyzerName);
        if (provider == null) {
            provider = loadAll(analyzerName);
            if (provider == null) {
                throw new RuntimeException(format(
                        "Could not create fulltext analyzer: %s. Could not find service provider %s[%s]",
                        analyzerName, AnalyzerProvider.class.getName(), analyzerName));
            }
        }
        Analyzer analyzer;
        try {
            analyzer = provider.createAnalyzer();
        } catch (Exception e) {
            throw new RuntimeException("Could not create fulltext analyzer: " + analyzerName, e);
        }
        Objects.requireNonNull(analyzer, "The '" + analyzerName + "' analyzer provider returned a null analyzer.");
        return analyzer;
    }

    private synchronized AnalyzerProvider loadAll(String analyzerName) {
        // Double-check the provider hasn't been loaded already before getting in here
        if (!analyzerProviders.containsKey(analyzerName)) {
            analyzerProviders.clear();
            for (AnalyzerProvider analyzerProvider : Services.loadAll(AnalyzerProvider.class)) {
                analyzerProviders.put(analyzerProvider.getName(), analyzerProvider);
            }
        }
        return analyzerProviders.get(analyzerName);
    }
}
