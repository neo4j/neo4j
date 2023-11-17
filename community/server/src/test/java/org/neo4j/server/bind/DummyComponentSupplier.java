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
package org.neo4j.server.bind;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.Supplier;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriInfo;

public class DummyComponentSupplier implements Supplier<DummyComponent> {
    @Context
    private UriInfo uriInfo;

    @Override
    public DummyComponent get() {
        // verify that container can inject into this class
        assertNotNull(uriInfo);

        return new DummyComponent();
    }
}
