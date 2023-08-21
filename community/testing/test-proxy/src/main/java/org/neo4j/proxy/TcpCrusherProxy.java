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
package org.neo4j.proxy;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.netcrusher.core.reactor.NioReactor;
import org.netcrusher.tcp.TcpCrusher;
import org.netcrusher.tcp.TcpCrusherBuilder;

/**
 * All methods are thread safe and can be used in safely in multithreading environment
 */
public class TcpCrusherProxy implements Neo4jProxy {
    private final Duration WAIT_FOR_OPERATION_TO_APPLY = Duration.ofSeconds(10);
    private final NioReactor reactor;
    private volatile boolean started;
    private final TcpCrusher tcpCrusher;

    private TcpCrusherProxy(ProxyConfiguration proxyConfiguration, NioReactor reactor) {
        started = true;
        this.reactor = reactor;
        this.tcpCrusher = TcpCrusherBuilder.builder()
                .withReactor(reactor)
                .withBindAddress(
                        proxyConfiguration.advertisedAddress().getHostName(),
                        proxyConfiguration.advertisedAddress().getPort())
                .withConnectAddress(
                        proxyConfiguration.listenAddress().getHostName(),
                        proxyConfiguration.listenAddress().getPort())
                .buildAndOpen();
        if (reactor == null) {
            throw new IllegalArgumentException("Nio reactor should be set");
        }
    }

    @Override
    public void freezeConnection() {
        tcpCrusher.freeze();
        waitForOperationToBeApplied(() -> !tcpCrusher.isFrozen());
    }

    @Override
    public void unfreezeConnection() {
        tcpCrusher.unfreeze();
        waitForOperationToBeApplied(tcpCrusher::isFrozen);
    }

    @Override
    public void closeAllConnection() {
        tcpCrusher.closeAllPairs();
        waitForOperationToBeApplied(() -> !tcpCrusher.getClientAddresses().isEmpty());
    }

    @Override
    public void stopAcceptingConnections() {
        tcpCrusher.close();
        waitForOperationToBeApplied(tcpCrusher::isOpen);
    }

    @Override
    public void startAcceptingConnections() {
        tcpCrusher.open();
        waitForOperationToBeApplied(() -> !tcpCrusher.isOpen());
    }

    @Override
    public ProxyConfiguration getProxyConfig() {
        return new ProxyConfiguration(tcpCrusher.getConnectAddress(), tcpCrusher.getBindAddress());
    }

    @Override
    public void close() {
        if (!started) {
            throw new IllegalStateException("Proxy is already stopped");
        }
        started = false;
        IOUtils.closeQuietly(tcpCrusher);
        IOUtils.closeQuietly(reactor);
    }

    private void waitForOperationToBeApplied(Supplier<Boolean> function) {
        var start = Instant.now();
        while (function.get()) {
            var now = Instant.now();
            if (Duration.between(start, now).compareTo(WAIT_FOR_OPERATION_TO_APPLY) > 0) {
                throw new IllegalStateException("Operation didn't complete for " + WAIT_FOR_OPERATION_TO_APPLY);
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<ProxyConfiguration> proxyConfiguration = Optional.empty();
        private Optional<NioReactor> reactor = Optional.empty();

        public Builder withProxyConfig(ProxyConfiguration proxyConfiguration) {
            this.proxyConfiguration = Optional.of(proxyConfiguration);
            return this;
        }

        public Neo4jProxy build() {
            var proxyConfig = this.proxyConfiguration.orElse(ProxyConfiguration.buildProxyConfig());
            try {
                var reactor = this.reactor.orElse(new NioReactor());
                return new TcpCrusherProxy(proxyConfig, reactor);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
