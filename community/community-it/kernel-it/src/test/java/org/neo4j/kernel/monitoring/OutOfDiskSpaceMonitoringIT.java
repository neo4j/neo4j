/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.monitoring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension(configurationCallback = "configuration")
public class OutOfDiskSpaceMonitoringIT {
    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        var listener = new DatabaseEventListenerAdapter() {
            @Override
            public void databaseOutOfDiskSpace(DatabaseEventContext event) {
                eventContext = event;
            }
        };
        builder.addDatabaseListener(listener);
    }

    private DatabaseEventContext eventContext;

    @Inject
    private DatabaseHealth databaseHealth;

    @Test
    void shouldPropagateOutOfDiskSpaceEventToRegisteredListener() {
        // When
        databaseHealth.outOfDiskSpace();

        // Then
        assertThat(eventContext).isNotNull();
        assertThat(eventContext.getDatabaseName()).isEqualTo(DEFAULT_DATABASE_NAME);
    }
}
