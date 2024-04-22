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
package org.neo4j.dbms.archive;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.dbms.archive.printer.ProgressPrinters.printStreamPrinter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.archive.LoggingArchiveProgressPrinter.PercentageCondition;
import org.neo4j.dbms.archive.printer.OutputProgressPrinter;
import org.neo4j.dbms.archive.printer.ProgressPrinters;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;
import org.neo4j.time.Clocks;

class ArchiveProgressPrinterTest {

    record Workload(Function<OutputProgressPrinter, List<String>> generator) {}

    private static Stream<Workload> workloads() {
        return Stream.of(
                new Workload(ArchiveProgressPrinterTest::executeSomeWork),
                new Workload(ArchiveProgressPrinterTest::executeSlowWorkload));
    }

    @ParameterizedTest
    @MethodSource("workloads")
    void printProgressStreamOutput(Workload workload) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(bout);
        OutputProgressPrinter outputPrinter = printStreamPrinter(printStream);

        var expected = workload.generator.apply(outputPrinter);
        printStream.flush();
        var actual = bout.toString().lines().collect(Collectors.toList());

        // pop ending empty entries due to the PrintStreamOutputPrinter
        actual.removeIf((s) -> s.equals(""));

        assertEachLineContains(actual, expected);
    }

    @ParameterizedTest
    @MethodSource("workloads")
    void printProgressEmptyReporter(Workload workload) {
        OutputProgressPrinter outputPrinter = ProgressPrinters.emptyPrinter();
        assertDoesNotThrow(() -> workload.generator.apply(outputPrinter));
    }

    @ParameterizedTest
    @MethodSource("workloads")
    void printProgressLogger(Workload workload) {
        try (AssertableLogProvider logProvider = new AssertableLogProvider()) {
            InternalLog providerLog = logProvider.getLog(ArchiveProgressPrinterTest.class);
            OutputProgressPrinter outputPrinter = ProgressPrinters.logProviderPrinter(providerLog);

            var expected = workload.generator.apply(outputPrinter);
            var actual = logProvider.serialize().lines().toList();
            assertEachLineContains(actual, expected);
        }
    }

    @Test
    void percentageConditionShouldBeReachedEveryPercent() {
        long maxBytes = 12345;
        var condition = new PercentageCondition(maxBytes);

        long progressSize = condition.bucket;
        long partialProgress = progressSize / 2;
        long progress = 1; // Start on offset so that we are never on an exact multiple of condition.bucket
        while (progress < maxBytes) {
            progress += partialProgress;
            assertThat(condition.updateAndCheckIfReached(progress)).isFalse();
            progress += (progressSize - partialProgress);
            assertThat(condition.updateAndCheckIfReached(progress)).isTrue();
        }
    }

    private static List<String> executeSomeWork(OutputProgressPrinter outputPrinter) {
        List<String> expected = new ArrayList<>();
        var clock = Clocks.fakeClock();
        ArchiveProgressPrinter progressPrinter =
                LoggingArchiveProgressPrinter.createProgressPrinter(outputPrinter, clock::instant);

        progressPrinter.maxBytes(1000);
        progressPrinter.maxFiles(10);

        progressPrinter.beginFile();
        progressPrinter.addBytes(5);
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.addBytes(50);
        progressPrinter.addBytes(50);
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes(100);
        progressPrinter.endFile();
        progressPrinter.beginFile();
        progressPrinter.printOnNextUpdate();
        progressPrinter.addBytes(100);
        progressPrinter.endFile();
        progressPrinter.done();
        progressPrinter.printProgress();

        expected.add(line(1, 10, 0.5)); // endFile
        expected.add(line(2, 10, 5.5)); // percentage
        expected.add(line(2, 10, 10.5)); // percentage
        expected.add(line(2, 10, 20.5)); // printOnNextUpdate
        expected.add(line(2, 10, 20.5)); // endFile
        expected.add(line(3, 10, 30.5)); // printOnNextUpdate
        expected.add(line(3, 10, 30.5)); // endFile
        expected.add(done(3, 305)); // done

        return expected;
    }

    private static List<String> executeSlowWorkload(OutputProgressPrinter outputProgressPrinter) {
        List<String> expected = new ArrayList<>();
        var clock = Clocks.fakeClock();
        ArchiveProgressPrinter progressPrinter =
                LoggingArchiveProgressPrinter.createProgressPrinter(outputProgressPrinter, clock::instant);
        var numFiles = 10;
        var numBytes = 10_000;

        try (var ignored = progressPrinter.startPrinting()) {
            progressPrinter.maxBytes(numBytes);
            progressPrinter.maxFiles(numFiles);

            progressPrinter.beginFile();
            // This disk is really slow
            for (int i = 0; i < numBytes; i++) {
                clock.forward(Duration.ofMillis(10));
                progressPrinter.addBytes(1);
            }
            progressPrinter.endFile();
        }

        for (int i = 1; i <= 100; i++) {
            expected.add(line(1, 10, i));
        }
        expected.add(line(1, 10, 100));
        expected.add(done(1, numBytes));
        return expected;
    }

    private static String line(int nFiles, int maxFiles, double percentage) {
        return format("Files: %d/%d, data: %4.1f%%", nFiles, maxFiles, percentage);
    }

    private static String done(int nFiles, int nBytes) {
        return format("Done: %d files, %s processed.", nFiles, ByteUnit.bytesToString(nBytes));
    }

    private static String niceMessage(String message, List<String> actual, List<String> expected) {
        var niceActual = new StringJoiner("\n\t,", "\t[", "\n\t]");
        actual.forEach(niceActual::add);

        var niceExpected = new StringJoiner("\n\t,", "\t[", "\n\t]");
        expected.forEach(niceExpected::add);
        return format(
                "%s\n\nExpected (size: %d): \n%s\nbut was (size: %d)\n%s\n",
                message, expected.size(), niceExpected.toString(), actual.size(), niceActual.toString());
    }

    private static void assertEachLineContains(List<String> actual, List<String> expected) {
        if (actual.size() != expected.size()) {
            fail(niceMessage("Different size.", actual, expected));
        }

        try {
            for (int i = 0; i < actual.size(); ++i) {
                assertThat(actual.get(i)).contains(expected.get(i));
            }
        } catch (AssertionError err) {
            fail(niceMessage(err.getMessage(), actual, expected));
        }
    }
}
