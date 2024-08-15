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
package org.neo4j.dbms.admissioncontrol;

public final class NoopAdmissionControlService implements AdmissionControlService {
    @Override
    public AdmissionControlToken requestToken() {
        return AdmissionControlToken.RELEASED;
    }

    @Override
    public AdmissionControlResponse awaitRelease(AdmissionControlToken token) {
        return AdmissionControlResponse.RELEASED;
    }

    @Override
    public boolean enabled() {
        return false;
    }
}
