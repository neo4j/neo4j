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
package org.neo4j.internal.id.configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.neo4j.internal.id.IdType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommunityIdTypeConfigurationProviderTest
{
    @Test
    void nonReusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( IdType.RELATIONSHIP );
        Assertions.assertFalse( typeConfiguration.allowAggressiveReuse(), "Relationship ids are not reusable." );
        assertEquals( IdTypeConfiguration.DEFAULT_GRAB_SIZE, typeConfiguration.getGrabSize(), "Relationship ids are not reusable." );
    }

    @ParameterizedTest
    @EnumSource( value = IdType.class, names = {"PROPERTY", "STRING_BLOCK", "ARRAY_BLOCK", "NODE_LABELS"} )
    void reusableTypeConfiguration( IdType reusableType )
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( reusableType );
        Assertions.assertTrue( typeConfiguration.allowAggressiveReuse() );
        assertEquals( IdTypeConfiguration.AGGRESSIVE_GRAB_SIZE, typeConfiguration.getGrabSize() );
    }

    private static IdTypeConfigurationProvider createIdTypeProvider()
    {
        return new CommunityIdTypeConfigurationProvider();
    }
}
