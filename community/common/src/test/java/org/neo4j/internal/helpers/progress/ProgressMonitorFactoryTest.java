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
package org.neo4j.internal.helpers.progress;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

class ProgressMonitorFactoryTest {
    @Test
    void shouldMapOneProgressRangeToAnother() {
        // given
        var targetProgression = new MutableLong();
        var target = new ProgressListener.Adapter() {
            @Override
            public void add(long progress) {
                targetProgression.add(progress);
            }
        };
        var mappedResolution = 10;
        var factory = ProgressMonitorFactory.mapped(target, mappedResolution);

        // when
        var totalCount = 100;
        var rng = ThreadLocalRandom.current();
        try (var source = factory.singlePart("Mapped", totalCount)) {
            for (int i = 0; i < totalCount; ) {
                int stride = rng.nextInt(1, 5);
                source.add(stride);
                i += stride;
            }
        }

        // then
        assertThat(targetProgression.longValue()).isEqualTo(mappedResolution);
    }

    @Test
    void shouldMapSubProgressesWithFractionsToAnother() {
        // given
        var targetProgression = new MutableLong();
        int totalResolution = 1_000;
        var target = new ProgressListener.Adapter() {
            @Override
            public void add(long progress) {
                targetProgression.add(progress);
            }

            @Override
            public int reportResolution() {
                return totalResolution;
            }
        };

        // when advancing 10% of the way (the "internal" total of the sub progress shouldn't matter at all)
        progressThrough(ProgressMonitorFactory.mappedFraction(target, 0.1f).singlePart("first", 1234));
        long progressionAfterFirst = targetProgression.longValue();
        progressThrough(ProgressMonitorFactory.mappedFraction(target, 0.5f).singlePart("second", 34783));
        long progressionAfterSecond = targetProgression.longValue();
        progressThrough(ProgressMonitorFactory.mappedFraction(target, 0.4f).singlePart("third", 289));
        long progressionAfterThird = targetProgression.longValue();

        // then
        assertThat(progressionAfterFirst).isEqualTo(100); // 10% of total
        assertThat(progressionAfterSecond).isEqualTo(600); // 10% + 50% (=60%) of total
        assertThat(progressionAfterThird).isEqualTo(totalResolution); // 10% + 50% + 40% (=100%) of total
    }

    @Test
    void shouldCallIndicatorListenerSinglePart() {
        // given
        var factory = ProgressMonitorFactory.textual(OutputStream.nullOutputStream(), false, 10, 1, 10); // 100 "dots"
        var total = 10_000;
        var listener = new ExternalListener(total);

        // when
        try (var progress = factory.singlePart("Test indicator listener", total, listener)) {
            for (int i = 0; i < total; i++) {
                progress.add(1);
            }
        }

        // then
        assertThat(listener.seenProgress).isEqualTo(total);
    }

    @Test
    void shouldCallIndicatorListenerMultiPart() {
        // given
        var factory = ProgressMonitorFactory.textual(OutputStream.nullOutputStream(), false, 10, 1, 10); // 100 "dots"
        var total = 10_000;
        var listener = new ExternalListener(total);

        // when
        var builder = factory.multipleParts("Test indicator listener", listener);
        var part1Total = total / 4;
        var part2Total = total - part1Total;
        var part1 = builder.progressForPart("Part 1", part1Total);
        var part2 = builder.progressForPart("Part 2", part2Total);
        try (var completor = builder.build();
                part2;
                part1) {
            for (int i = 0; i < part1Total; i++) {
                part1.add(1);
            }
            for (int i = 0; i < part2Total; i++) {
                part2.add(1);
            }
        }

        // then
        assertThat(listener.seenProgress).isEqualTo(total);
    }

    @Test
    void getProgressShouldNotExceedTotalCount() {
        try (var listener =
                new ProgressListener.SinglePartProgressListener(Indicator.NONE, 100, (progress, total) -> {})) {

            listener.add(60);
            listener.add(60);

            assertThat(listener.getProgress()).isEqualTo(100);
        }
    }

    @Test
    void getProgressShouldReflectTotalCountAfterClose() {
        try (var listener =
                new ProgressListener.SinglePartProgressListener(Indicator.NONE, 100, (progress, total) -> {})) {

            listener.add(40);
            listener.close();

            assertThat(listener.getProgress()).isEqualTo(100);
        }
    }

    private void progressThrough(ProgressListener listener) {
        try (listener) {
            // just progress some and let it be completed in its close() call, doesn't quite matter
            for (int i = 0; i < 10; i++) {
                listener.add(1);
            }
        }
    }

    private static class ExternalListener implements ProgressMonitorFactory.IndicatorListener {
        private final long total;
        private long seenProgress;

        ExternalListener(long total) {
            this.total = total;
        }

        @Override
        public void update(long progress, long total) {
            assertThat(progress).isGreaterThanOrEqualTo(seenProgress);
            assertThat(total).isEqualTo(this.total);
            seenProgress = progress;
        }
    }
}
