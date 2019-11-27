/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.factory;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseConfig;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;

class AccessCapabilityFactoryTest
{
    private final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private final NamedDatabaseId namedDatabaseId = databaseIdRepository.getRaw( "foo" );

    private final DatabaseConfig readWriteConfig = new DatabaseConfig( Config.defaults( read_only, false ), namedDatabaseId );
    private final DatabaseConfig readOnlyConfig = new DatabaseConfig( Config.defaults( read_only, true ), namedDatabaseId );

    @Test
    void shouldCreateConfigDependentFactoryForReadWriteConfig()
    {
        var factory = AccessCapabilityFactory.configDependent();

        var accessCapability = factory.newAccessCapability( readWriteConfig );

        assertThat( accessCapability ).isInstanceOf( CanWrite.class );
        assertDoesNotThrow( accessCapability::assertCanWrite );
    }

    @Test
    void shouldCreateConfigDependentFactoryForReadOnlyConfig()
    {
        var factory = AccessCapabilityFactory.configDependent();

        var accessCapability = factory.newAccessCapability( readOnlyConfig );

        assertThat( accessCapability ).isInstanceOf( ReadOnly.class );
        assertThrows( WriteOperationsNotAllowedException.class, accessCapability::assertCanWrite );
    }

    @Test
    void shouldCreateFixedFactory()
    {
        var accessCapability1 = new CanWrite();
        var accessCapability2 = new ReadOnly();

        var factory1 = AccessCapabilityFactory.fixed( accessCapability1 );
        var factory2 = AccessCapabilityFactory.fixed( accessCapability2 );

        assertEquals( accessCapability1, factory1.newAccessCapability( readWriteConfig ) );
        assertEquals( accessCapability1, factory1.newAccessCapability( readOnlyConfig ) );

        assertEquals( accessCapability2, factory2.newAccessCapability( readWriteConfig ) );
        assertEquals( accessCapability2, factory2.newAccessCapability( readOnlyConfig ) );
    }
}
