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
package org.neo4j.server.security.auth;

import static org.neo4j.configuration.GraphDatabaseSettings.auth_lock_time;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_max_failed_attempts;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.kernel.impl.security.User;

public class RateLimitedAuthenticationStrategy implements AuthenticationStrategy {
    private final Clock clock;
    private final long lockDurationMs;
    private final int maxFailedAttempts;

    private class AuthenticationMetadata {
        private final AtomicInteger failedAuthAttempts = new AtomicInteger();
        private final AtomicLong lastFailedAttemptTime = new AtomicLong();

        boolean authenticationPermitted() {
            return maxFailedAttempts <= 0
                    || // amount of attempts is not limited
                    failedAuthAttempts.get() < maxFailedAttempts
                    || // less failed attempts than configured
                    clock.millis() >= lastFailedAttemptTime.get() + lockDurationMs; // auth lock duration expired
        }

        void authSuccess() {
            failedAuthAttempts.set(0);
        }

        void authFailed() {
            // Publish the timestamp before the count: a concurrent authenticationPermitted() that
            // observes the incremented count must also observe this timestamp, otherwise it could
            // read a stale (e.g. zero) time, conclude the lock duration expired, and admit the
            // attempt. Keep the timestamp monotonic so a failure that read an earlier clock value
            // cannot overwrite a later one and shorten the lock.
            lastFailedAttemptTime.accumulateAndGet(clock.millis(), Math::max);
            failedAuthAttempts.incrementAndGet();
        }
    }

    /**
     * Tracks authentication state for each user
     */
    private final ConcurrentMap<String, AuthenticationMetadata> authenticationData = new ConcurrentHashMap<>();

    public RateLimitedAuthenticationStrategy(Clock clock, Config config) {
        this(clock, config.get(auth_lock_time), config.get(auth_max_failed_attempts));
    }

    RateLimitedAuthenticationStrategy(Clock clock, Duration lockDuration, int maxFailedAttempts) {
        this.clock = clock;
        this.lockDurationMs = lockDuration.toMillis();
        this.maxFailedAttempts = maxFailedAttempts;
    }

    @Override
    public AuthenticationResult authenticate(User user, byte[] password) {
        AuthenticationMetadata authMetadata = authMetadataFor(user.name());

        if (!authMetadata.authenticationPermitted()) {
            return AuthenticationResult.TOO_MANY_ATTEMPTS;
        }

        if (user.credential().value().matchesPassword(password)) {
            authMetadata.authSuccess();
            return AuthenticationResult.SUCCESS;
        } else {
            authMetadata.authFailed();
            return AuthenticationResult.FAILURE;
        }
    }

    private AuthenticationMetadata authMetadataFor(String username) {
        AuthenticationMetadata authMeta = authenticationData.get(username);

        if (authMeta == null) {
            authMeta = new AuthenticationMetadata();
            AuthenticationMetadata preExisting = authenticationData.putIfAbsent(username, authMeta);
            if (preExisting != null) {
                authMeta = preExisting;
            }
        }

        return authMeta;
    }
}
