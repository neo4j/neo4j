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
package org.neo4j.graphdb.facade;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class DatabaseManagementServiceFactoryTest {
    @Inject
    private TestDirectory testDirectory;

    private final ExternalDependencies deps = mock(ExternalDependencies.class, RETURNS_MOCKS);

    @BeforeEach
    void setup() {
        when(deps.monitors()).thenReturn(new Monitors());
        when(deps.dependencies()).thenReturn(null);
    }

    @Test
    void shouldThrowAppropriateExceptionIfStartFails() {
        Config config = Config.defaults(GraphDatabaseSettings.neo4j_home, testDirectory.absolutePath());
        RuntimeException startupError = new RuntimeException();
        DatabaseManagementServiceFactory factory = newFaultyGraphDatabaseFacadeFactory(startupError, null);
        assertThatThrownBy(() -> factory.build(config, false, deps)).rootCause().isEqualTo(startupError);
    }

    @Test
    void shouldThrowAppropriateExceptionIfBothStartAndShutdownFail() {
        Config config = Config.defaults(GraphDatabaseSettings.neo4j_home, testDirectory.absolutePath());
        RuntimeException startupException = new RuntimeException();
        RuntimeException shutdownException = new RuntimeException();

        DatabaseManagementServiceFactory factory =
                newFaultyGraphDatabaseFacadeFactory(startupException, shutdownException);
        RuntimeException initException = assertThrows(RuntimeException.class, () -> factory.build(config, false, deps));

        assertTrue(initException.getMessage().startsWith("Error starting "));
        assertThat(initException).hasRootCause(startupException);
        assertThat(initException.getSuppressed()[0]).hasRootCause(shutdownException);
    }

    private static DatabaseManagementServiceFactory newFaultyGraphDatabaseFacadeFactory(
            final RuntimeException startupError, RuntimeException shutdownError) {
        return new DatabaseManagementServiceFactory(DbmsInfo.UNKNOWN, CommunityEditionModule::new) {
            @Override
            protected GlobalModule createGlobalModule(
                    Config config, boolean daemonMode, ExternalDependencies dependencies) {
                LifeSupport lifeSupport = new LifeSupport();
                lifeSupport.add(new PoisonedLifecycleMember(startupError, shutdownError));

                return new GlobalModule(config, dbmsInfo, daemonMode, dependencies) {
                    @Override
                    public LifeSupport createLife() {
                        return lifeSupport;
                    }
                };
            }
        };
    }

    private static class PoisonedLifecycleMember extends LifecycleAdapter {
        private final RuntimeException startupException;
        private final RuntimeException shutdownException;

        private PoisonedLifecycleMember(RuntimeException startupException, RuntimeException shutdownException) {
            this.startupException = startupException;
            this.shutdownException = shutdownException;
        }

        @Override
        public void start() {
            throw startupException;
        }

        @Override
        public void shutdown() {
            if (shutdownException != null) {
                throw shutdownException;
            }
        }
    }
}
