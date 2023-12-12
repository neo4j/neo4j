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
import static org.neo4j.internal.helpers.progress.TextualIndicator.DEFAULT_DOTS_PER_GROUP;
import static org.neo4j.internal.helpers.progress.TextualIndicator.DEFAULT_GROUPS_PER_LINE;
import static org.neo4j.internal.helpers.progress.TextualIndicator.DEFAULT_NUM_LINES;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

class IndicatorTest {
    @Test
    void shouldIncludeDeltaTimes() {
        // given
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter(bout);
        FakeClock clock = new FakeClock();
        TextualIndicator indicator = new TextualIndicator(
                "Test", out, true, clock, 'D', DEFAULT_DOTS_PER_GROUP, DEFAULT_GROUPS_PER_LINE, DEFAULT_NUM_LINES);

        // when
        int line = 0;
        clock.forward(1, TimeUnit.SECONDS);
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 10%
        clock.forward(100, TimeUnit.MILLISECONDS);
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 20%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 30%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 40%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 50%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 60%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 70%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 80%
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 90%
        clock.forward(3, TimeUnit.SECONDS);
        indicator.progress(DEFAULT_DOTS_PER_GROUP * line, DEFAULT_DOTS_PER_GROUP * ++line); // 100%

        // then
        out.flush();
        String output = bout.toString();
        assertThat(output).contains("10% D1s");
        assertThat(output).contains("20% D100ms");
        assertThat(output).contains("40% D0ms");
        assertThat(output).contains("50% D0ms");
        assertThat(output).contains("60% D0ms");
        assertThat(output).contains("70% D0ms");
        assertThat(output).contains("80% D0ms");
        assertThat(output).contains("90% D0ms");
        assertThat(output).contains("100% D3s");
    }

    @Test
    void shouldConfigureNumberOfDotsAndLines() {
        // given
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter(bout);
        int dotsPerGroup = 5;
        int lines = 4;
        TextualIndicator indicator =
                new TextualIndicator("Test", out, false, Clocks.nanoClock(), ' ', dotsPerGroup, 1, lines);

        // when
        indicator.progress(0, dotsPerGroup * lines);

        // then
        out.flush();
        String output = bout.toString();
        assertThat(output).contains(".....  25%");
        assertThat(output).contains(".....  50%");
        assertThat(output).contains(".....  75%");
        assertThat(output).contains("..... 100%");
    }

    @Test
    void shouldConfigureNumberOfDotsAndGroupsLines() {
        // given
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter(bout);
        int dotsPerGroup = 5;
        int groupsPerLine = 3;
        int lines = 5;
        TextualIndicator indicator =
                new TextualIndicator("Test", out, false, Clocks.nanoClock(), ' ', dotsPerGroup, groupsPerLine, lines);

        // when
        indicator.progress(0, groupsPerLine * dotsPerGroup * lines);

        // then
        out.flush();
        String output = bout.toString();
        assertThat(output).contains("..... ..... .....  20%");
        assertThat(output).contains("..... ..... .....  40%");
        assertThat(output).contains("..... ..... .....  60%");
        assertThat(output).contains("..... ..... .....  80%");
        assertThat(output).contains("..... ..... ..... 100%");
    }

    @Test
    void shouldPrintMarks() {
        // given
        var bout = new ByteArrayOutputStream();
        var out = new PrintWriter(bout);
        var indicator = new TextualIndicator("Test", out, false, Clocks.nanoClock(), ' ', 10, 1, 2);

        // when
        indicator.progress(0, 5);
        indicator.mark('1');
        indicator.progress(5, 12);
        indicator.mark('2');
        indicator.progress(12, 20);

        // then
        out.flush();
        var output = bout.toString();
        assertThat(output).contains(".....1....  50%");
        assertThat(output).contains("..2....... 100%");
    }
}
