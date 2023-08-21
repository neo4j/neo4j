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
package org.neo4j.shell.test.bolt;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.BaseSession;
import org.neo4j.driver.BookmarkManager;
import org.neo4j.driver.Driver;
import org.neo4j.driver.ExecutableQuery;
import org.neo4j.driver.Metrics;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.reactive.ReactiveSession;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.types.TypeSystem;

public class FakeDriver implements Driver {
    public List<SessionConfig> sessionConfigs = new LinkedList<>();

    @Override
    public ExecutableQuery executableQuery(String s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BookmarkManager executableQueryBookmarkManager() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEncrypted() {
        return false;
    }

    @Override
    public Session session() {
        return new FakeSession();
    }

    @Override
    public Session session(SessionConfig sessionConfig) {
        sessionConfigs.add(sessionConfig);
        return new FakeSession();
    }

    @Override
    public <T extends BaseSession> T session(Class<T> aClass, SessionConfig sessionConfig) {
        sessionConfigs.add(sessionConfig);
        return (T) new FakeSession();
    }

    @Override
    public <T extends BaseSession> T session(Class<T> aClass, SessionConfig sessionConfig, AuthToken authToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Neo4jException {}

    @Override
    public CompletionStage<Void> closeAsync() {
        return null;
    }

    @Override
    public Metrics metrics() {
        return null;
    }

    @Override
    public boolean isMetricsEnabled() {
        return false;
    }

    @Override
    public RxSession rxSession() {
        return null;
    }

    @Override
    public RxSession rxSession(SessionConfig sessionConfig) {
        return null;
    }

    @Override
    public ReactiveSession reactiveSession(SessionConfig sessionConfig) {
        return null;
    }

    @Override
    public AsyncSession asyncSession() {
        return null;
    }

    @Override
    public AsyncSession asyncSession(SessionConfig sessionConfig) {
        return null;
    }

    @Override
    public TypeSystem defaultTypeSystem() {
        return null;
    }

    @Override
    public void verifyConnectivity() {}

    @Override
    public CompletionStage<Void> verifyConnectivityAsync() {
        return null;
    }

    @Override
    public boolean verifyAuthentication(AuthToken authToken) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsSessionAuth() {
        return false;
    }

    @Override
    public boolean supportsMultiDb() {
        return true;
    }

    @Override
    public CompletionStage<Boolean> supportsMultiDbAsync() {
        return completedFuture(true);
    }
}
