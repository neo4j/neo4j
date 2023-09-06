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
package org.neo4j.kernel.info;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.neo4j.kernel.info.JvmChecker.INCOMPATIBLE_JVM_VERSION_WARNING;
import static org.neo4j.kernel.info.JvmChecker.initialMemorySettingWarning;
import static org.neo4j.kernel.info.JvmChecker.maxMemorySettingWarning;
import static org.neo4j.logging.LogAssertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLog;

class JVMCheckerTest {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final InternalLog log = logProvider.getLog("test");

    @Test
    void shouldIssueWarningWhenUsingHotspotServerVmVersion12() {
        new JvmChecker(log, new CannedJvmMetadataRepository("Java HotSpot(TM) 64-Bit Server VM", "12"))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).containsMessages(INCOMPATIBLE_JVM_VERSION_WARNING);
    }

    @Test
    void shouldNotIssueWarningWhenUsingHotspotServerVmVersion17() {
        new JvmChecker(log, new CannedJvmMetadataRepository("Java HotSpot(TM) 64-Bit Server VM", "17"))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).doesNotContainMessage(INCOMPATIBLE_JVM_VERSION_WARNING);
    }

    @Test
    void shouldNotIssueWarningWhenUsingVm21() {
        new JvmChecker(log, new CannedJvmMetadataRepository("Java HotSpot(TM) 64-Bit Server VM", "21"))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).doesNotContainMessage(INCOMPATIBLE_JVM_VERSION_WARNING);
    }

    @Test
    void shouldIssueWarningWhenUsingUnsupportedJvmVersion() {
        new JvmChecker(log, new CannedJvmMetadataRepository("Java HotSpot(TM) 64-Bit Server VM", "22.33.44.55"))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).containsMessages(INCOMPATIBLE_JVM_VERSION_WARNING);
    }

    @Test
    void warnAboutMissingInitialHeapSize() {
        new JvmChecker(
                        log,
                        new CannedJvmMetadataRepository(
                                "Java HotSpot(TM) 64-Bit Server VM", "11.0.2+9", singletonList("-XMx"), 12, 23))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).containsMessages(initialMemorySettingWarning(12));
    }

    @Test
    void warnAboutMissingMaximumHeapSize() {
        new JvmChecker(
                        log,
                        new CannedJvmMetadataRepository(
                                "Java HotSpot(TM) 64-Bit Server VM", "11", singletonList("-XMs"), 12, 23))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).containsMessages(maxMemorySettingWarning(23));
    }

    @Test
    void warnAboutMissingHeapSizes() {
        new JvmChecker(log, new CannedJvmMetadataRepository("Java HotSpot(TM) 64-Bit Server VM", "11.0.1"))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).containsMessages(initialMemorySettingWarning(1));
        assertThat(logProvider).containsMessages(maxMemorySettingWarning(2));
    }

    @Test
    void doNotWarnAboutMissingHeapSizesWhenOptionsSpecified() {
        new JvmChecker(
                        log,
                        new CannedJvmMetadataRepository(
                                "Java HotSpot(TM) 64-Bit Server VM", "11.0.2", asList("-xMx", "-xmS"), 1, 2))
                .checkJvmCompatibilityAndIssueWarning();

        assertThat(logProvider).doesNotContainMessage(initialMemorySettingWarning(1));
        assertThat(logProvider).doesNotContainMessage(maxMemorySettingWarning(2));
    }
}
