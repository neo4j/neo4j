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
package org.neo4j.capabilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.logging.InternalLog;

class CapabilityProviderContextTest {

    @Test
    void shouldResolveComponents() {
        var log = mock(InternalLog.class);
        var config = mock(Configuration.class);
        var dbms = mock(DatabaseManagementService.class);
        var caps = mock(Capabilities.class);

        var deps = new CapabilityProviderDependencies();
        deps.register(InternalLog.class, () -> log);
        deps.register(Configuration.class, () -> config);
        deps.register(DatabaseManagementService.class, () -> dbms);
        deps.register(Capabilities.class, () -> caps);

        var ctx = new CapabilityProviderContext(deps);

        assertThat(ctx.log()).isSameAs(log);
        assertThat(ctx.config()).isSameAs(config);
        assertThat(ctx.dbms()).isSameAs(dbms);
        assertThat(ctx.get(Capabilities.class)).isSameAs(caps);
    }
}
