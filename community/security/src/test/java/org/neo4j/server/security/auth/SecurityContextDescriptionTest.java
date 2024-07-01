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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.server.security.auth.SecurityTestUtils.authToken;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.impl.api.security.OverriddenAccessMode;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.systemgraph.BasicSystemGraphRealm;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.time.Clocks;

class SecurityContextDescriptionTest {
    private SecurityContext context;

    @BeforeEach
    void setup() throws Throwable {
        SystemGraphRealmHelper realmHelper = spy(new SystemGraphRealmHelper(null, new SecureHasher()));
        BasicSystemGraphRealm realm = new BasicSystemGraphRealm(
                realmHelper, new RateLimitedAuthenticationStrategy(Clocks.systemClock(), Config.defaults()));
        User user = new User("johan", "id", credentialFor("bar"), false, false);
        doReturn(user).when(realmHelper).getUser("johan");
        context = realm.login(authToken("johan", "bar"), EMBEDDED_CONNECTION)
                .authorize(LoginContext.IdLookup.EMPTY, DEFAULT_DATABASE_NAME, CommunitySecurityLog.NULL_LOG);
    }

    @Test
    void shouldMakeNiceDescription() {
        assertThat(context.description()).isEqualTo("user 'johan' with FULL");
    }

    @Test
    void shouldMakeNiceDescriptionWithMode() {
        SecurityContext modified = context.withMode(AccessMode.Static.WRITE);
        assertThat(modified.description()).isEqualTo("user 'johan' with WRITE");
    }

    @Test
    void shouldMakeNiceDescriptionRestricted() {
        SecurityContext restricted = context.withMode(new RestrictedAccessMode(context.mode(), AccessMode.Static.READ));
        assertThat(restricted.description()).isEqualTo("user 'johan' with FULL restricted to READ");
    }

    @Test
    void shouldMakeNiceDescriptionOverridden() {
        SecurityContext overridden = context.withMode(new OverriddenAccessMode(context.mode(), AccessMode.Static.READ));
        assertThat(overridden.description()).isEqualTo("user 'johan' with FULL overridden by READ");
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabled() {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        assertThat(disabled.description()).isEqualTo("AUTH_DISABLED with FULL");
    }

    @Test
    void shouldMakeNiceDescriptionAuthDisabledAndRestricted() {
        SecurityContext disabled = SecurityContext.AUTH_DISABLED;
        SecurityContext restricted =
                disabled.withMode(new RestrictedAccessMode(disabled.mode(), AccessMode.Static.READ));
        assertThat(restricted.description()).isEqualTo("AUTH_DISABLED with FULL restricted to READ");
    }
}
