/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.enterprise.id;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfigurationProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith( Parameterized.class )
public class EnterpriseIdTypeConfigurationProviderTest
{
    private final IdType reusableType;

    @Parameterized.Parameters
    public static List<IdType> data()
    {
        return Arrays.asList( IdType.PROPERTY,
                IdType.STRING_BLOCK,
                IdType.ARRAY_BLOCK,
                IdType.NODE,
                IdType.RELATIONSHIP,
                IdType.NODE_LABELS );
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
        Config config = Config.defaults( params );
        return new EnterpriseIdTypeConfigurationProvider( config );
    }
}
