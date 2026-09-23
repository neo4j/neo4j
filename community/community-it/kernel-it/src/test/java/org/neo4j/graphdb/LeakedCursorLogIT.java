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
package org.neo4j.graphdb;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.fail_on_leaked_cursors;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.log_leaked_cursors;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.track_cursor_close;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
class LeakedCursorLogIT {
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private GraphDatabaseFacade db;

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(log_leaked_cursors, true)
                .setConfig(fail_on_leaked_cursors, false)
                .setConfig(track_cursor_close, false)
                .setInternalLogProvider(logProvider);
    }

    @Test
    void shouldLogLeakedCursorKind() {
        // Given a database where leaking cursors is reported rather than fatal
        InternalTransaction tx = db.beginTransaction(KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED);
        tx.kernelTransaction().cursors().allocateNodeCursor(NULL_CONTEXT);

        // When the transaction commits while holding an un-returned cursor
        tx.commit();

        // Then the leak is logged, naming the cursor kind and the settings that capture a stack trace
        LogAssertions.assertThat(logProvider)
                .containsMessageWithAll("Not all allocated cursors were returned", "node: 1", "track_cursor_close");
    }
}
