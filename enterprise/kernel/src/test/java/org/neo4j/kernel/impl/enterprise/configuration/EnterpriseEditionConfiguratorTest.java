/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise.configuration;


import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class EnterpriseEditionConfiguratorTest
{

    @After
    public void setUp()
    {
        IdType.RELATIONSHIP.setAllowAggressiveReuse( false );
        IdType.SCHEMA.setAllowAggressiveReuse( false );
    }

    @Test
    public void configureIdTypeToBeReusableByDefault()
    {
        new EnterpriseEditionConfigurator( new Config() ).configure();
        assertTrue( IdType.RELATIONSHIP.allowAggressiveReuse() );
    }

    @Test
    public void skipConfigurationOfIdTypesWhenNotConfigured()
    {
        Config config = new Config( stringMap( EnterpriseEditionSettings.idTypesToReuse.name(), "" ) );
        new EnterpriseEditionConfigurator( config ).configure();
        assertFalse( IdType.RELATIONSHIP.allowAggressiveReuse() );
    }

    @Test
    public void configureMultipleTypesToBeReusable()
    {
        Map<String,String> configMap = getConfigWithMutipleIdTypes();
        new EnterpriseEditionConfigurator( new Config(configMap) ).configure();
        assertTrue( IdType.RELATIONSHIP.allowAggressiveReuse() );
        assertTrue( IdType.SCHEMA.allowAggressiveReuse() );
    }

    private Map<String,String> getConfigWithMutipleIdTypes()
    {
        String[] idTypes = {IdType.RELATIONSHIP.getName().name(), IdType.SCHEMA.getName().name()};
        return stringMap( EnterpriseEditionSettings.idTypesToReuse.name(), StringUtils.join( idTypes, "," ) );
    }

}