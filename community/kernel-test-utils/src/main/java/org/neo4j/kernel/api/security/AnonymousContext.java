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
package org.neo4j.kernel.api.security;

import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;

import org.neo4j.internal.kernel.api.security.AbstractSecurityLog;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.database.PrivilegeDatabaseReference;

/** Controls the capabilities of a KernelTransaction. */
public class AnonymousContext extends LoginContext {
    private final AccessMode accessMode;

    private AnonymousContext(AccessMode accessMode) {
        super(AuthSubject.ANONYMOUS, EMBEDDED_CONNECTION);
        this.accessMode = accessMode;
    }

    public static AnonymousContext access() {
        return new AnonymousContext(AccessMode.Static.ACCESS);
    }

    public static AnonymousContext read() {
        return new AnonymousContext(AccessMode.Static.READ);
    }

    public static AnonymousContext write() {
        return new AnonymousContext(AccessMode.Static.WRITE);
    }

    public static AnonymousContext writeToken() {
        return new AnonymousContext(AccessMode.Static.TOKEN_WRITE);
    }

    public static AnonymousContext writeOnly() {
        return new AnonymousContext(AccessMode.Static.WRITE_ONLY);
    }

    public static AnonymousContext full() {
        return new AnonymousContext(AccessMode.Static.FULL);
    }

    @Override
    public SecurityContext authorize(
            IdLookup idLookup, PrivilegeDatabaseReference dbReference, AbstractSecurityLog securityLog) {
        return new SecurityContext(subject(), accessMode, connectionInfo(), dbReference.name());
    }
}
