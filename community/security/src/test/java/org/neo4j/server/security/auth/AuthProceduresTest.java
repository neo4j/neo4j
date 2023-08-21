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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.AuthenticationResult;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.server.security.auth.AuthProcedures.UserResult;

public class AuthProceduresTest {
    private AuthProcedures procedures;

    @BeforeEach
    void setup() {
        AuthSubject subject = mock(AuthSubject.class);
        when(subject.executingUser()).thenReturn("pearl");
        when(subject.getAuthenticationResult()).thenReturn(AuthenticationResult.SUCCESS);

        SecurityContext ctx = new SecurityContext(
                subject, AccessMode.Static.FULL, ClientConnectionInfo.EMBEDDED_CONNECTION, "database");

        procedures = new AuthProcedures();
        procedures.securityContext = ctx;
    }

    @Test
    void shouldReturnSecurityContext() {
        List<UserResult> infoList = procedures.showCurrentUser().collect(Collectors.toList());
        assertThat(infoList.size()).isEqualTo(1);

        UserResult row = infoList.get(0);
        assertThat(row.username).isEqualTo("pearl");
        assertThat(row.roles).isNull();
        assertThat(row.flags).isEmpty();
    }
}
