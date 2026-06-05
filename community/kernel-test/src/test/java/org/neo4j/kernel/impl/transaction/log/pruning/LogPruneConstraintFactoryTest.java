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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory.getThresholdByType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdConfigParser.ThresholdConfigValue;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

class LogPruneConstraintFactoryTest {

    private FileSystemAbstraction fsa;
    private SystemNanoClock clock;
    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp() {
        fsa = new DefaultFileSystemAbstraction();
        clock = Clocks.nanoClock();
        logProvider = new AssertableLogProvider();
    }

    @Test
    void configuringFilesThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("files", 25));
        assertThat(threshold).isInstanceOf(FileCountThreshold.class);
        assertEquals("25 files", threshold.toString());
    }

    @Test
    void configuringSizeThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("size", 16000));
        assertThat(threshold).isInstanceOf(FileSizeThreshold.class);
        assertEquals("16000 size", threshold.toString());
    }

    @Test
    void configuringTxsThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("txs", 4000));
        assertThat(threshold).isInstanceOf(EntryCountThreshold.class);
        assertEquals("4000 entries", threshold.toString());
    }

    @Test
    void configuringEntriesThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("entries", 4000));
        assertThat(threshold).isInstanceOf(EntryCountThreshold.class);
        assertEquals("4000 entries", threshold.toString());
    }

    @Test
    void configuringHoursThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("hours", 100));
        assertThat(threshold).isInstanceOf(EntryTimespanThreshold.class);
        assertEquals("100 hours", threshold.toString());
    }

    @Test
    void configuringDaysThreshold() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("days", 100_000));
        assertThat(threshold).isInstanceOf(EntryTimespanThreshold.class);
        assertEquals("100000 days", threshold.toString());
    }

    @Test
    void configuringDaysThresholdWithMaxSize() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("days", 100_000, ByteUnit.mebiBytes(500)));
        assertThat(threshold).isInstanceOf(EntryTimespanThreshold.class);
        assertEquals("100000 days 524288000 size", threshold.toString());
    }

    @Test
    void configuringBackupThresholdWithMinMaxSize() {
        LogPruneThreshold threshold =
                getThreshold(new ThresholdConfigValue("backup", ByteUnit.mebiBytes(100), ByteUnit.mebiBytes(50)));
        assertThat(threshold).isInstanceOf(BackupThreshold.class);
        assertEquals("backup 0 52428800 size 104857600 size", threshold.toString());
    }

    @Test
    void configuringBackupThresholdWithMaxSize() {
        LogPruneThreshold threshold = getThreshold(new ThresholdConfigValue("backup", ByteUnit.mebiBytes(100)));
        assertThat(threshold).isInstanceOf(BackupThreshold.class);
        ((BackupThreshold) threshold).setBackupAppendIndex(42);

        assertEquals("backup 42 104857600 size", threshold.toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"keep_all", "true"})
    void noPruningConfigValuesShortCircuitToNoPruning(String configValue) {
        LogPruneStrategy strategy = new LogPruneStrategyFactory()
                .strategyFromConfigValue(
                        fsa,
                        mock(LogFiles.class),
                        logProvider,
                        clock,
                        configValue,
                        mock(TransactionLogFileInformation.class));
        assertThat(strategy).isSameAs(LogPruneStrategyFactory.NO_PRUNING);
    }

    private LogPruneThreshold getThreshold(ThresholdConfigValue configValue) {
        return getThresholdByType(fsa, logProvider, clock, configValue);
    }
}
