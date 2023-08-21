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
package org.neo4j.logging.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.InternalLogProvider;

class SimpleLogServiceTest {
    @Test
    void shouldCreateDuplicatingLogProviderIfDuplicationIsEnabled() {
        var userLog = mock(InternalLogProvider.class);
        var internalLog = mock(InternalLogProvider.class);
        var service = new SimpleLogService(userLog, internalLog, true);

        assertThat(service.getInternalLogProvider()).isSameAs(internalLog);

        var serviceUserLog = service.getUserLogProvider();
        assertThat(serviceUserLog).isInstanceOf(DuplicatingLogProvider.class);
        assertThat(((DuplicatingLogProvider) serviceUserLog).getTargetLogProviders())
                .contains(userLog, internalLog);
    }

    @Test
    void shouldNotCreateDuplicatingLogProviderIfDuplicationIsDisabled() {
        var userLog = mock(InternalLogProvider.class);
        var internalLog = mock(InternalLogProvider.class);
        var service = new SimpleLogService(userLog, internalLog, false);

        assertThat(service.getInternalLogProvider()).isSameAs(internalLog);
        assertThat(service.getUserLogProvider()).isSameAs(userLog);
    }
}
