/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.id;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.IdTypeConfiguration;
import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class EnterpriseIdTypeConfigurationProviderTest
{

    private IdType reusableType;

    @Parameterized.Parameters
    public static List<Object[]> data()
    {
        return Arrays.asList( new Object[]{IdType.PROPERTY},
                new Object[]{IdType.STRING_BLOCK},
                new Object[]{IdType.ARRAY_BLOCK},
                new Object[]{IdType.NODE},
                new Object[]{IdType.RELATIONSHIP},
                new Object[]{IdType.NODE_LABELS} );
    }

    public EnterpriseIdTypeConfigurationProviderTest( IdType reusableType )
    {
        this.reusableType = reusableType;
    }

    @Test
    public void nonReusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( IdType.SCHEMA );
        assertFalse( "Schema record ids are not reusable.", typeConfiguration.allowAggressiveReuse() );
        assertEquals( "Schema record ids are not reusable.", 1024, typeConfiguration.getGrabSize() );
    }

    @Test
    public void reusableTypeConfiguration()
    {
        IdTypeConfigurationProvider provider = createIdTypeProvider();
        IdTypeConfiguration typeConfiguration = provider.getIdTypeConfiguration( reusableType );
        assertTrue( typeConfiguration.allowAggressiveReuse() );
        assertEquals( 50000, typeConfiguration.getGrabSize() );
    }

    private IdTypeConfigurationProvider createIdTypeProvider()
    {
        Map<String,String> params = MapUtil.stringMap( EnterpriseEditionSettings.idTypesToReuse.name(),
                IdType.NODE + "," + IdType.RELATIONSHIP );
        Config config = new Config( params );
        return new EnterpriseIdTypeConfigurationProvider( config );
    }

}
