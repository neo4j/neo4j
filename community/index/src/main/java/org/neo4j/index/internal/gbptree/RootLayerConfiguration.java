/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import java.io.IOException;
import org.neo4j.index.internal.gbptree.RootMappingLayout.RootMappingValue;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;

public abstract sealed class RootLayerConfiguration<ROOT_KEY>
        permits RootLayerConfiguration.SingleRootLayerConfiguration,
                RootLayerConfiguration.MultiRootLayerConfiguration {
    public static RootLayerConfiguration<SingleRoot> singleRoot() {
        return new SingleRootLayerConfiguration();
    }

    public static <ROOT_KEY> RootLayerConfiguration<ROOT_KEY> multipleRoots(
            KeyLayout<ROOT_KEY> rootKeyLayout, int rootMappingCacheSize) {
        return new MultiRootLayerConfiguration<>(rootKeyLayout, rootMappingCacheSize);
    }

    abstract <VALUE, KEY> RootLayer<ROOT_KEY, KEY, VALUE> buildRootLayer(
            RootLayerSupport rootLayerSupport,
            Layout<KEY, VALUE> dataLayout,
            boolean created,
            CursorContext cursorContext,
            CursorContextFactory contextFactory)
            throws IOException;

    abstract Layout<ROOT_KEY, RootMappingValue> rootLayout();

    static final class SingleRootLayerConfiguration extends RootLayerConfiguration<SingleRoot> {
        @Override
        <VALUE, KEY> RootLayer<SingleRoot, KEY, VALUE> buildRootLayer(
                RootLayerSupport rootLayerSupport,
                Layout<KEY, VALUE> dataLayout,
                boolean created,
                CursorContext cursorContext,
                CursorContextFactory contextFactory)
                throws IOException {
            return new SingleRootLayer<>(rootLayerSupport, dataLayout, created, cursorContext);
        }

        @Override
        Layout<SingleRoot, RootMappingValue> rootLayout() {
            return null;
        }
    }

    static final class MultiRootLayerConfiguration<ROOT_KEY> extends RootLayerConfiguration<ROOT_KEY> {
        private final Layout<ROOT_KEY, RootMappingValue> rootKeyLayout;
        private final int rootMappingCacheSize;

        MultiRootLayerConfiguration(KeyLayout<ROOT_KEY> rootKeyLayout, int rootMappingCacheSize) {
            this.rootKeyLayout = new RootMappingLayout<>(rootKeyLayout);
            this.rootMappingCacheSize = rootMappingCacheSize;
        }

        @Override
        <VALUE, KEY> RootLayer<ROOT_KEY, KEY, VALUE> buildRootLayer(
                RootLayerSupport rootLayerSupport,
                Layout<KEY, VALUE> dataLayout,
                boolean created,
                CursorContext cursorContext,
                CursorContextFactory contextFactory)
                throws IOException {
            return new MultiRootLayer<>(
                    rootLayerSupport,
                    rootKeyLayout,
                    dataLayout,
                    rootMappingCacheSize,
                    created,
                    cursorContext,
                    contextFactory);
        }

        @Override
        Layout<ROOT_KEY, RootMappingValue> rootLayout() {
            return rootKeyLayout;
        }
    }
}
