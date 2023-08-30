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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.commons.lang3.mutable.MutableLong;
import org.assertj.core.api.Condition;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.common.DependencyResolver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@ExtendWith(RandomExtension.class)
public class GBPTreeWithUndefinedValuesTest {

    public static final int MAX_NUMBERS = 2000;
    public static final int WRITE_ROUNDS = 1000;

    @Inject
    PageCache pageCache;

    @Inject
    FileSystemAbstraction fileSystem;

    @Inject
    TestDirectory testDirectory;

    @Inject
    RandomSupport randomSupport;

    @Test
    void seekerShouldNotSeeUndefinedValues() throws IOException {
        try (var tree = makeTree(
                new ConditionalReadLayoutFactory<>(GBPTreeWithUndefinedValuesTest::evenLong, MutableLong.class))) {
            var valueCount = 100;
            try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                for (int i = 0; i < valueCount; i++) {
                    writer.put(new MutableLong(i), new MutableLong(i));
                }
            }

            try (var seeker = tree.seek(new MutableLong(0), new MutableLong(valueCount), CursorContext.NULL_CONTEXT)) {
                while (seeker.next()) {
                    assertThat(seeker.value())
                            .is(new Condition<>(
                                    GBPTreeWithUndefinedValuesTest::evenLong, "seeker should see only even values"));
                }
            }
        }
    }

    @Test
    void overwriteUndefinedValue() throws IOException {
        try (var tree = makeTree(
                new ConditionalReadLayoutFactory<>(GBPTreeWithUndefinedValuesTest::evenLong, MutableLong.class))) {
            try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                writer.put(new MutableLong(0), new MutableLong(1));
            }

            try (var seeker = tree.seek(new MutableLong(0), new MutableLong(1), CursorContext.NULL_CONTEXT)) {
                assertThat(seeker.next()).isFalse();
            }

            try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                writer.put(new MutableLong(0), new MutableLong(2));
            }

            try (var seeker = tree.seek(new MutableLong(0), new MutableLong(1), CursorContext.NULL_CONTEXT)) {
                assertThat(seeker.next()).isTrue();
                assertThat(seeker.key()).isEqualTo(new MutableLong(0));
                assertThat(seeker.value()).isEqualTo(new MutableLong(2));
            }
        }
    }

    @Test
    void removeUndefinedValue() throws IOException {
        try (var tree = makeTree(
                new ConditionalReadLayoutFactory<>(GBPTreeWithUndefinedValuesTest::evenLong, MutableLong.class))) {
            try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                writer.put(new MutableLong(0), new MutableLong(1));
            }

            try (var seeker = tree.seek(new MutableLong(0), new MutableLong(1), CursorContext.NULL_CONTEXT)) {
                assertThat(seeker.next()).isFalse();
            }

            try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                assertThat(writer.remove(new MutableLong(0))).isNull();
            }

            try (var seeker = tree.seek(new MutableLong(0), new MutableLong(1), CursorContext.NULL_CONTEXT)) {
                assertThat(seeker.next()).isFalse();
            }
        }
    }

    @Test
    void concurrentReadWriteWithUndefinedValues() throws Throwable {
        try (var tree = makeTree(
                new ConditionalReadLayoutFactory<>(GBPTreeWithUndefinedValuesTest::overHalf, MutableLong.class))) {

            var race = new Race();

            var finished = new AtomicBoolean(false);
            race.addContestant(throwing(() -> {
                for (int i = 0; i < WRITE_ROUNDS; i++) {
                    try (var writer = tree.writer(CursorContext.NULL_CONTEXT)) {
                        for (int j = 0; j < 100; j++) {
                            var number = randomSupport.nextLong(MAX_NUMBERS);
                            if (randomSupport.nextLong(100) < 15) {
                                writer.remove(new MutableLong(number));
                            } else {
                                writer.put(new MutableLong(number), new MutableLong(number));
                            }
                        }
                    }
                }
                finished.set(true);
            }));
            race.addContestants(10, throwing(() -> {
                while (!finished.get()) {
                    try (var seeker = tree.seek(
                            new MutableLong(randomSupport.nextLong(MAX_NUMBERS)),
                            new MutableLong(randomSupport.nextLong(MAX_NUMBERS)),
                            CursorContext.NULL_CONTEXT)) {
                        while (seeker.next()) {
                            assertThat(seeker.value())
                                    .is(new Condition<>(
                                            GBPTreeWithUndefinedValuesTest::overHalf,
                                            "seeker should see only half of the values"));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }));

            race.go(5, TimeUnit.MINUTES);
        }
    }

    private static boolean overHalf(MutableLong m) {
        return m.longValue() > (MAX_NUMBERS / 2);
    }

    private static boolean evenLong(MutableLong value) {
        return value.longValue() % 2 == 0;
    }

    private GBPTree<MutableLong, MutableLong> makeTree(ConditionalReadLayoutFactory<?> treeNodeLayoutFactory) {
        var layout = SimpleLongLayout.longLayout()
                .withFixedSize(true)
                .withKeyPadding(randomSupport.nextInt(1000))
                .build();
        var builder = new GBPTreeBuilder<SingleRoot, MutableLong, MutableLong>(
                        pageCache, fileSystem, testDirectory.file("index"), layout)
                .with(treeNodeLayoutFactory);
        return builder.build();
    }

    static class ConditionalReadLayoutFactory<V> implements TreeNodeLayoutFactory {

        private final Predicate<V> predicate;
        private final Class<V> vClass;

        ConditionalReadLayoutFactory(Predicate<V> predicate, Class<V> vClass) {
            this.predicate = predicate;
            this.vClass = vClass;
        }

        @Override
        public int getPriority() {
            return 0;
        }

        @Override
        public TreeNodeSelector createSelector(ImmutableSet<OpenOption> openOptions) {
            return layout -> new TreeNodeSelector.Factory() {
                @Override
                public <KEY, VALUE> LeafNodeBehaviour<KEY, VALUE> createLeafBehaviour(
                        int pageSize,
                        Layout<KEY, VALUE> layout,
                        OffloadStore<KEY, VALUE> offloadStore,
                        DependencyResolver dependencyResolver) {
                    return new LeafNodeFixedSize<>(pageSize, layout) {
                        @Override
                        public void keyValueAt(
                                PageCursor cursor,
                                KEY intoKey,
                                ValueHolder<VALUE> intoValue,
                                int pos,
                                CursorContext cursorContext)
                                throws IOException {
                            super.keyValueAt(cursor, intoKey, intoValue, pos, cursorContext);
                            intoValue.defined = predicate.test(vClass.cast(intoValue.value));
                        }

                        @Override
                        public ValueHolder<VALUE> valueAt(
                                PageCursor cursor, ValueHolder<VALUE> value, int pos, CursorContext cursorContext)
                                throws IOException {
                            var valueHolder = super.valueAt(cursor, value, pos, cursorContext);
                            valueHolder.defined = predicate.test(vClass.cast(value.value));
                            return valueHolder;
                        }
                    };
                }

                @Override
                public <KEY, VALUE> InternalNodeBehaviour<KEY> createInternalBehaviour(
                        int pageSize,
                        Layout<KEY, VALUE> layout,
                        OffloadStore<KEY, VALUE> offloadStore,
                        DependencyResolver dependencyResolver) {
                    return new InternalNodeFixedSize<>(pageSize, layout);
                }

                @Override
                public byte formatIdentifier() {
                    return -1;
                }

                @Override
                public byte formatVersion() {
                    return -1;
                }
            };
        }
    }
}
