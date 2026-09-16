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

import java.util.HashSet;
import java.util.Set;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ssl.SslPolicyScope;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.ssl.SslPolicy;

public class DefaultSslPolicyProvider implements SslPolicyProvider {
    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final boolean enableManualReload;
    private final boolean enableFileChangeReload;
    private final InternalLogProvider logProvider;
    private SslPolicyLoader policyLoader;
    private final Set<SslPolicyChangeListener> listeners = new HashSet<SslPolicyChangeListener>();

    public DefaultSslPolicyProvider(
            FileSystemAbstraction fileSystem,
            Config config,
            boolean enableManualReload,
            boolean enableFileChangeReload,
            InternalLogProvider logProvider) {
        this.fileSystem = fileSystem;
        this.config = config;
        this.enableManualReload = enableManualReload;
        this.enableFileChangeReload = enableFileChangeReload;
        this.logProvider = logProvider;
        policyLoader = SslPolicyLoader.create(fileSystem, config, logProvider);
    }

    @Override
    public void reloadPolicies() {
        if (enableManualReload) {
            doReload();
        }
    }

    @Override
    public void reloadPoliciesOnFileChange() {
        if (enableFileChangeReload) {
            doReload();
        }
    }

    private void doReload() {
        policyLoader = SslPolicyLoader.create(fileSystem, config, logProvider);
        notifyListeners();
    }

    private synchronized void notifyListeners() {
        for (final var scope : SslPolicyScope.values()) {
            if (policyLoader.hasPolicyForSource(scope)) {
                final var policy = policyLoader.getPolicy(scope);
                listeners.forEach(listener -> listener.policyChanged(scope, policy));
            }
        }
    }

    @Override
    public boolean hasPolicyForScope(SslPolicyScope scope) {
        return policyLoader.hasPolicyForSource(scope);
    }

    @Override
    public SslPolicy getPolicy(SslPolicyScope scope) {
        return policyLoader.getPolicy(scope);
    }

    @Override
    public synchronized void addPolicyChangeListener(SslPolicyChangeListener listener) {
        listeners.add(listener);
    }

    @Override
    public synchronized void removePolicyChangeListener(SslPolicyChangeListener listener) {
        listeners.remove(listener);
    }
}
