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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.util.VisibleForTesting;

@VisibleForTesting
public interface GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> extends IdProvider.IdProviderVisitor {
    void meta(Meta meta);

    void treeState(Pair<TreeState, TreeState> statePair);

    void beginTree(boolean dataTree);

    void beginLevel(int level);

    void beginNode(long pageId, boolean isLeaf, long generation, int keyCount);

    void rootKey(ROOT_KEY key, boolean isLeaf, long offloadId);

    void rootMapping(long id, long generation);

    void key(DATA_KEY key, boolean isLeaf, long offloadId);

    void value(DATA_VALUE value);

    void child(long child);

    void position(int i);

    void endNode(long pageId);

    void endLevel(int level);

    void endTree(boolean dataTree);

    void historyStart();

    void historyEnd();

    void historicalValue(long version, ValueHolder<DATA_VALUE> value);

    class Adaptor<ROOT_KEY, DATA_KEY, DATA_VALUE> implements GBPTreeVisitor<ROOT_KEY, DATA_KEY, DATA_VALUE> {
        @Override
        public void meta(Meta meta) {}

        @Override
        public void treeState(Pair<TreeState, TreeState> statePair) {}

        @Override
        public void beginTree(boolean dataTree) {}

        @Override
        public void beginLevel(int level) {}

        @Override
        public void beginNode(long pageId, boolean isLeaf, long generation, int keyCount) {}

        @Override
        public void rootKey(ROOT_KEY key, boolean isLeaf, long offloadId) {}

        @Override
        public void rootMapping(long id, long generation) {}

        @Override
        public void key(DATA_KEY key, boolean isLeaf, long offloadId) {}

        @Override
        public void value(DATA_VALUE value) {}

        @Override
        public void child(long child) {}

        @Override
        public void position(int i) {}

        @Override
        public void endNode(long pageId) {}

        @Override
        public void endLevel(int level) {}

        @Override
        public void endTree(boolean dataTree) {}

        @Override
        public void historyStart() {}

        @Override
        public void historyEnd() {}

        @Override
        public void historicalValue(long version, ValueHolder<DATA_VALUE> value) {}

        @Override
        public void beginFreelistPage(long pageId) {}

        @Override
        public void endFreelistPage(long pageId) {}

        @Override
        public void freelistEntry(long pageId, long generation, int pos) {}

        @Override
        public void freelistEntryFromReleaseCache(long pageId) {}
    }
}
