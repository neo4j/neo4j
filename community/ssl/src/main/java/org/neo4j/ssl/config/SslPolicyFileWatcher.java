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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyConfig;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.fs.watcher.resource.WatchedResource;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

/**
 * Watches the directories backing configured {@link SslPolicyConfig}s (base directory, private key,
 * public certificate, trusted directory and revoked directory) and automatically triggers
 * {@link SslPolicyProvider#reloadPoliciesOnFileChange()} whenever one of them changes on disk.
 * <p>
 * This provides the same effect as calling {@code dbms.security.reloadTLS()}, but without requiring
 * an authenticated client to invoke a procedure whenever certificates are rotated.
 * <p>
 * Multiple file system events resulting from a single certificate rotation (e.g. a private key and a
 * certificate being updated separately, or a delete-then-recreate rotation strategy) are collapsed
 * using a short debounce delay before the reload is actually performed.
 */
public class SslPolicyFileWatcher extends LifecycleAdapter implements FileWatchEventListener {
    private static final long DEBOUNCE_MILLIS = 2000;

    private final FileWatcher fileWatcher;
    private final Config config;
    private final SslPolicyProvider sslPolicyProvider;
    private final JobScheduler jobScheduler;
    private final InternalLog log;
    private final long debounceMillis;

    private final Set<WatchedResource> watchedResources = new LinkedHashSet<>();
    private volatile JobHandle<?> pendingReload;

    public SslPolicyFileWatcher(
            FileWatcher fileWatcher,
            Config config,
            SslPolicyProvider sslPolicyProvider,
            JobScheduler jobScheduler,
            InternalLogProvider logProvider) {
        this(fileWatcher, config, sslPolicyProvider, jobScheduler, logProvider, DEBOUNCE_MILLIS);
    }

    @VisibleForTesting
    SslPolicyFileWatcher(
            FileWatcher fileWatcher,
            Config config,
            SslPolicyProvider sslPolicyProvider,
            JobScheduler jobScheduler,
            InternalLogProvider logProvider,
            long debounceMillis) {
        this.fileWatcher = fileWatcher;
        this.config = config;
        this.sslPolicyProvider = sslPolicyProvider;
        this.jobScheduler = jobScheduler;
        this.log = logProvider.getLog(SslPolicyFileWatcher.class);
        this.debounceMillis = debounceMillis;
    }

    @Override
    public void start() {
        Set<Path> directoriesToWatch = collectDirectoriesToWatch();
        for (Path directory : directoriesToWatch) {
            watchDirectory(directory);
        }
        fileWatcher.addFileWatchEventListener(this);

        if (watchedResources.isEmpty()) {
            log.warn("Automatic TLS certificate reload is enabled, but no existing SSL policy directories "
                    + "were found to watch. Verify that 'db.filewatcher.enabled' is true and that the "
                    + "configured SSL policy directories exist.");
        } else {
            log.info(
                    "Watching %d director%s for automatic TLS certificate reload.",
                    watchedResources.size(), watchedResources.size() == 1 ? "y" : "ies");
        }
    }

    @Override
    public void stop() {
        fileWatcher.removeFileWatchEventListener(this);
        cancelPendingReload();
        try {
            IOUtils.closeAll(watchedResources);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            watchedResources.clear();
        }
    }

    @Override
    public void fileModified(WatchKey key, String fileName) {
        scheduleReload();
    }

    @Override
    public void fileDeleted(WatchKey key, String fileName) {
        scheduleReload();
    }

    /**
     * Cancels any not-yet-fired reload and schedules a new one {@code debounceMillis} from now. Since every
     * call restarts the delay, {@link #reload()} only actually runs once the directories have been quiet for
     * {@code debounceMillis} - i.e. this is a trailing-edge debounce, not a fixed polling interval. A burst of
     * events (e.g. a key and certificate written separately) collapses into a single reload shortly after the
     * last one lands.
     */
    private synchronized void scheduleReload() {
        cancelPendingReload();
        pendingReload = jobScheduler.schedule(
                Group.SECURITY_MAINTAINENCE,
                JobMonitoringParams.systemJob("Automatic TLS certificate reload"),
                this::reload,
                debounceMillis,
                TimeUnit.MILLISECONDS);
    }

    private synchronized void cancelPendingReload() {
        if (pendingReload != null) {
            pendingReload.cancel();
            pendingReload = null;
        }
    }

    private void reload() {
        try {
            sslPolicyProvider.reloadPoliciesOnFileChange();
            log.info("Automatically reloaded TLS certificates and configuration following a file system change.");
        } catch (Exception e) {
            log.warn(
                    "Failed to automatically reload TLS certificates and configuration following a file system "
                            + "change. A subsequent change will trigger another attempt.",
                    e);
        }
    }

    private Set<Path> collectDirectoriesToWatch() {
        Set<Path> directories = new LinkedHashSet<>();
        for (SslPolicyConfig policyConfig :
                config.getGroups(SslPolicyConfig.class).values()) {
            if (!config.get(policyConfig.enabled)) {
                continue;
            }
            addParent(directories, config.get(policyConfig.private_key));
            addParent(directories, config.get(policyConfig.public_certificate));
            directories.add(config.get(policyConfig.base_directory));
            directories.add(config.get(policyConfig.trusted_dir));
            directories.add(config.get(policyConfig.revoked_dir));
        }
        return directories;
    }

    private static void addParent(Set<Path> directories, Path file) {
        if (file != null && file.getParent() != null) {
            directories.add(file.getParent());
        }
    }

    private void watchDirectory(Path directory) {
        if (directory == null || !Files.isDirectory(directory)) {
            return;
        }
        try {
            watchedResources.add(fileWatcher.watch(directory));
        } catch (IOException e) {
            log.warn("Failed to watch directory '" + directory + "' for automatic TLS certificate reload.", e);
        }
    }
}
