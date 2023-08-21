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
package org.neo4j.kernel.impl.factory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;

class AccessCapabilityFactoryTest {
    private final DatabaseReadOnlyChecker readWriteChecker = DatabaseReadOnlyChecker.writable();
    private final DatabaseReadOnlyChecker readOnlyChecker = DatabaseReadOnlyChecker.readOnly();

    @Test
    void shouldCreateConfigDependentFactoryForReadWriteConfig() {
        var factory = AccessCapabilityFactory.configDependent();

        var accessCapability = factory.newAccessCapability(readWriteChecker);

        assertThat(accessCapability).isInstanceOf(CanWrite.class);
        assertDoesNotThrow(accessCapability::assertCanWrite);
    }

    @Test
    void shouldCreateConfigDependentFactoryForReadOnlyConfig() {
        var factory = AccessCapabilityFactory.configDependent();

        var accessCapability = factory.newAccessCapability(readOnlyChecker);

        assertThat(accessCapability).isInstanceOf(ReadOnly.class);
        assertThrows(WriteOperationsNotAllowedException.class, accessCapability::assertCanWrite);
    }

    @Test
    void shouldCreateFixedFactory() {
        var accessCapability1 = CanWrite.INSTANCE;
        var accessCapability2 = ReadOnly.INSTANCE;
        var accessCapability3 = ReadReplica.INSTANCE;

        var factory1 = AccessCapabilityFactory.fixed(accessCapability1);
        var factory2 = AccessCapabilityFactory.fixed(accessCapability2);
        var factory3 = AccessCapabilityFactory.fixed(accessCapability3);

        assertEquals(accessCapability1, factory1.newAccessCapability(readWriteChecker));
        assertEquals(accessCapability1, factory1.newAccessCapability(readOnlyChecker));

        assertEquals(accessCapability2, factory2.newAccessCapability(readWriteChecker));
        assertEquals(accessCapability2, factory2.newAccessCapability(readOnlyChecker));

        assertEquals(accessCapability3, factory3.newAccessCapability(readWriteChecker));
        assertEquals(accessCapability3, factory3.newAccessCapability(readOnlyChecker));
    }
}
