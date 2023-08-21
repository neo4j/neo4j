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
package org.neo4j.dbms.systemgraph;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import org.neo4j.logging.Level;
import org.neo4j.values.storable.DurationValue;

public final class DriverSettings {
    enum Keys {
        SSL_ENFORCED,
        CONNECTION_TIMEOUT,
        CONNECTION_MAX_LIFETIME,
        CONNECTION_POOL_ACQUISITION_TIMEOUT,
        CONNECTION_POOL_IDLE_TEST,
        CONNECTION_POOL_MAX_SIZE,
        LOGGING_LEVEL;

        @Override
        public String toString() {
            return name().toLowerCase();
        }
    }

    private final Boolean sslEnforced;
    private final Duration connectionTimeout;
    private final Duration connectionMaxLifetime;
    private final Duration connectionPoolAcquisitionTimeout;
    private final Duration connectionPoolIdleTest;
    private final Integer connectionPoolMaxSize;
    private final Level loggingLevel;
    private final String sslPolicy;

    private DriverSettings(
            Boolean sslEnforced,
            Duration connectionTimeout,
            Duration connectionMaxLifetime,
            Duration connectionPoolAcquisitionTimeout,
            Duration connectionPoolIdleTest,
            Integer connectionPoolMaxSize,
            Level loggingLevel,
            String sslPolicy) {
        this.sslEnforced = sslEnforced;
        this.connectionTimeout = connectionTimeout;
        this.connectionMaxLifetime = connectionMaxLifetime;
        this.connectionPoolAcquisitionTimeout = connectionPoolAcquisitionTimeout;
        this.connectionPoolIdleTest = connectionPoolIdleTest;
        this.connectionPoolMaxSize = connectionPoolMaxSize;
        this.loggingLevel = loggingLevel;
        this.sslPolicy = sslPolicy;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Optional<Boolean> isSslEnforced() {
        return Optional.ofNullable(sslEnforced);
    }

    public Optional<Duration> connectionTimeout() {
        return Optional.ofNullable(connectionTimeout);
    }

    public Optional<Duration> connectionMaxLifetime() {
        return Optional.ofNullable(connectionMaxLifetime);
    }

    public Optional<Duration> connectionPoolAcquisitionTimeout() {
        return Optional.ofNullable(connectionPoolAcquisitionTimeout);
    }

    public Optional<Duration> connectionPoolIdleTest() {
        return Optional.ofNullable(connectionPoolIdleTest);
    }

    public Optional<Integer> connectionPoolMaxSize() {
        return Optional.ofNullable(connectionPoolMaxSize);
    }

    public Optional<Level> loggingLevel() {
        return Optional.ofNullable(loggingLevel);
    }

    public Optional<String> sslPolicy() {
        return Optional.ofNullable(sslPolicy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DriverSettings that = (DriverSettings) o;
        return Objects.equals(sslEnforced, that.sslEnforced)
                && Objects.equals(connectionTimeout, that.connectionTimeout)
                && Objects.equals(connectionMaxLifetime, that.connectionMaxLifetime)
                && Objects.equals(connectionPoolAcquisitionTimeout, that.connectionPoolAcquisitionTimeout)
                && Objects.equals(connectionPoolIdleTest, that.connectionPoolIdleTest)
                && Objects.equals(connectionPoolMaxSize, that.connectionPoolMaxSize)
                && loggingLevel == that.loggingLevel
                && Objects.equals(sslPolicy, that.sslPolicy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sslEnforced,
                connectionTimeout,
                connectionMaxLifetime,
                connectionPoolAcquisitionTimeout,
                connectionPoolIdleTest,
                connectionPoolMaxSize,
                loggingLevel);
    }

    static class Builder {
        private Boolean sslEnabled;
        private Duration connectionTimeout;
        private Duration connectionMaxLifetime;
        private Duration connectionPoolAcquisitionTimeout;
        private Duration connectionPoolIdleTest;
        private Integer connectionPoolMaxSize;
        private Level loggingLevel;
        private String sslPolicy;

        private Builder() {}

        Builder withSslEnforced(boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        Builder withConnectionTimeout(DurationValue connectionTimeout) {
            this.connectionTimeout = convertDurationValue(connectionTimeout, Keys.CONNECTION_TIMEOUT.toString());
            return this;
        }

        Builder withConnectionMaxLifeTime(DurationValue connectionMaxLifetime) {
            this.connectionMaxLifetime =
                    convertDurationValue(connectionMaxLifetime, Keys.CONNECTION_MAX_LIFETIME.toString());
            return this;
        }

        Builder withConnectionPoolAcquisitionTimeout(DurationValue connectionPoolAcquisitionTimeout) {
            this.connectionPoolAcquisitionTimeout = convertDurationValue(
                    connectionPoolAcquisitionTimeout, Keys.CONNECTION_POOL_ACQUISITION_TIMEOUT.toString());
            return this;
        }

        Builder withConnectionPoolIdleTest(DurationValue connectionPoolIdleTest) {
            this.connectionPoolIdleTest =
                    convertDurationValue(connectionPoolIdleTest, Keys.CONNECTION_POOL_IDLE_TEST.toString());
            return this;
        }

        Builder withConnectionPoolMaxSize(int connectionPoolMaxSize) {
            this.connectionPoolMaxSize = connectionPoolMaxSize;
            return this;
        }

        Builder withLoggingLevel(Level loggingLevel) {
            this.loggingLevel = loggingLevel;
            return this;
        }

        Builder withSSLPolicy(String sslPolicy) {
            this.sslPolicy = sslPolicy;
            return this;
        }

        DriverSettings build() {
            return new DriverSettings(
                    sslEnabled,
                    connectionTimeout,
                    connectionMaxLifetime,
                    connectionPoolAcquisitionTimeout,
                    connectionPoolIdleTest,
                    connectionPoolMaxSize,
                    loggingLevel,
                    sslPolicy);
        }

        private Duration convertDurationValue(DurationValue durationValue, String durationName) {
            // Need a max value which is guaranteed not to be estimated (i.e. not have months or years in the duration)
            //  in order to safely convert to java duration.
            if (durationValue.compareTo(DurationValue.duration(Duration.ofHours(24))) > 0) {
                throw new IllegalStateException(String.format(
                        "Driver setting %s is a duration of %s. "
                                + "This is greater than the max of 24 hours! Please reduce it.",
                        durationName, durationValue.prettyPrint()));
            }
            var seconds = durationValue.get(ChronoUnit.SECONDS);
            return Duration.ofSeconds(seconds);
        }
    }
}
