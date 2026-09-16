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
package org.neo4j.ssl.config;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.neo4j.configuration.ssl.SslPolicyScope.TESTING;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.io.fs.watcher.DefaultFileSystemWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.utils.TestDirectory;

/**
 * Tier A ({@link #burstOfEventsCollapsesIntoASingleScheduledReload()}, {@link #reloadFailureIsLoggedAndDoesNotPropagate()})
 * exercises the debounce/scheduling logic in isolation using {@link OnDemandJobScheduler}, so it is deterministic and
 * does not depend on real timing or a real file system watcher - c.f. how
 * {@code com.neo4j.server.security.enterprise.auth.OidcSettingsTest} tests delayed scheduling.
 * <p>
 * Tier B ({@link #watchesConfiguredPolicyDirectoriesAndReloadsOnRealFileChange()},
 * {@link #stopsWatchingAndNoLongerReloadsAfterStop()}) drives a real {@link DefaultFileSystemWatcher} against real
 * directories on disk, in the same style as {@code DefaultFileSystemWatcherTest}, using a short debounce delay so the
 * test stays fast.
 */
@TestDirectoryExtension
class SslPolicyFileWatcherTest {
    @Inject
    private TestDirectory testDirectory;

    @Test
    void burstOfEventsCollapsesIntoASingleScheduledReload() {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        CountingSslPolicyProvider sslPolicyProvider = new CountingSslPolicyProvider();
        SslPolicyFileWatcher watcher =
                newWatcher(FileWatcher.SILENT_WATCHER, Config.defaults(), sslPolicyProvider, jobScheduler, 2000);

        // A burst of events from a single certificate rotation (key + cert written separately, then the old
        // ones removed) should cancel-and-reschedule rather than queuing up multiple reloads.
        watcher.fileModified(null, "public.crt");
        watcher.fileModified(null, "private.key");
        watcher.fileDeleted(null, "public.crt");

        assertThat(jobScheduler.getJobs()).hasSize(1);

        jobScheduler.runJob();

        assertThat(sslPolicyProvider.reloadCount()).isEqualTo(1);
    }

    @Test
    void reloadFailureIsLoggedAndDoesNotPropagate() {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        SslPolicyFileWatcher watcher = newWatcher(
                FileWatcher.SILENT_WATCHER, Config.defaults(), new ThrowingSslPolicyProvider(), jobScheduler, 2000);

        watcher.fileModified(null, "public.crt");

        assertThatCode(jobScheduler::runJob).doesNotThrowAnyException();
    }

    @Test
    void watchesConfiguredPolicyDirectoriesAndReloadsOnRealFileChange() throws Exception {
        Path baseDir = testDirectory.directory("certificates");
        Config config = configWithPolicyDirectory(baseDir);
        CountingSslPolicyProvider sslPolicyProvider = new CountingSslPolicyProvider();

        try (DefaultFileSystemWatcher fileWatcher =
                        new DefaultFileSystemWatcher(FileSystems.getDefault().newWatchService());
                ThreadPoolJobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            SslPolicyFileWatcher watcher = newWatcher(fileWatcher, config, sslPolicyProvider, jobScheduler, 50);

            startWatchingInBackground(fileWatcher);
            watcher.start();
            try {
                Files.writeString(baseDir.resolve("public.crt"), "new certificate bytes");

                assertEventually(sslPolicyProvider::reloadCount, count -> count >= 1, 10, SECONDS);
            } finally {
                watcher.stop();
                fileWatcher.close();
            }
        }
    }

    @Test
    void stopsWatchingAndNoLongerReloadsAfterStop() throws Exception {
        Path baseDir = testDirectory.directory("certificates");
        Config config = configWithPolicyDirectory(baseDir);
        CountingSslPolicyProvider sslPolicyProvider = new CountingSslPolicyProvider();

        try (DefaultFileSystemWatcher fileWatcher =
                        new DefaultFileSystemWatcher(FileSystems.getDefault().newWatchService());
                ThreadPoolJobScheduler jobScheduler = new ThreadPoolJobScheduler()) {
            SslPolicyFileWatcher watcher = newWatcher(fileWatcher, config, sslPolicyProvider, jobScheduler, 50);

            startWatchingInBackground(fileWatcher);
            watcher.start();
            watcher.stop();

            Files.writeString(baseDir.resolve("public.crt"), "new certificate bytes");

            // No reload should ever be scheduled once stopped, so the count should stay at zero even after
            // waiting comfortably longer than the debounce delay.
            Thread.sleep(500);
            assertThat(sslPolicyProvider.reloadCount()).isZero();
        }
    }

    private static Config configWithPolicyDirectory(Path baseDir) {
        SslPolicyConfig policyConfig = SslPolicyConfig.forScope(TESTING);
        return Config.newBuilder()
                .set(policyConfig.enabled, true)
                .set(policyConfig.base_directory, baseDir)
                .build();
    }

    private static void startWatchingInBackground(FileWatcher fileWatcher) {
        Thread watcherThread = new Thread(
                () -> {
                    try {
                        fileWatcher.startWatching();
                    } catch (InterruptedException ignored) {
                        // expected on shutdown
                    }
                },
                "ssl-policy-file-watcher-test");
        watcherThread.setDaemon(true);
        watcherThread.start();
    }

    private static SslPolicyFileWatcher newWatcher(
            FileWatcher fileWatcher,
            Config config,
            SslPolicyProvider sslPolicyProvider,
            org.neo4j.scheduler.JobScheduler jobScheduler,
            long debounceMillis) {
        return new SslPolicyFileWatcher(
                fileWatcher, config, sslPolicyProvider, jobScheduler, NullLogProvider.getInstance(), debounceMillis);
    }

    private static class CountingSslPolicyProvider implements SslPolicyProvider {
        private final AtomicInteger reloadCount = new AtomicInteger();

        int reloadCount() {
            return reloadCount.get();
        }

        @Override
        public void reloadPolicies() {
            throw new UnsupportedOperationException("Only reloadPoliciesOnFileChange() is exercised by this test");
        }

        @Override
        public void reloadPoliciesOnFileChange() {
            reloadCount.incrementAndGet();
        }

        @Override
        public boolean hasPolicyForScope(SslPolicyScope scope) {
            return false;
        }

        @Override
        public SslPolicy getPolicy(SslPolicyScope scope) {
            return null;
        }

        @Override
        public void addPolicyChangeListener(SslPolicyChangeListener listener) {}

        @Override
        public void removePolicyChangeListener(SslPolicyChangeListener listener) {}
    }

    private static class ThrowingSslPolicyProvider implements SslPolicyProvider {
        @Override
        public void reloadPolicies() {
            throw new UnsupportedOperationException("Only reloadPoliciesOnFileChange() is exercised by this test");
        }

        @Override
        public void reloadPoliciesOnFileChange() {
            throw new RuntimeException("boom");
        }

        @Override
        public boolean hasPolicyForScope(SslPolicyScope scope) {
            return false;
        }

        @Override
        public SslPolicy getPolicy(SslPolicyScope scope) {
            return null;
        }

        @Override
        public void addPolicyChangeListener(SslPolicyChangeListener listener) {}

        @Override
        public void removePolicyChangeListener(SslPolicyChangeListener listener) {}
    }
}
