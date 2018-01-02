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
package org.neo4j.kernel.impl.enterprise.configuration;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.configuration.Config;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.idTypesToReuse;

public class EnterpriseEditionSettingsTest
{
    @Test
    public void idTypesToReuseAllowedValues()
    {
        for ( IdType type : IdType.values() )
        {
            if ( type == IdType.NODE || type == IdType.RELATIONSHIP )
            {
                assertIdTypesToReuseAllows( type );
            }
            else
            {
                assertIdTypesToReuseDisallows( type );
            }
        }

        assertIdTypesToReuseAllows( IdType.NODE, IdType.RELATIONSHIP );
        assertIdTypesToReuseAllows( IdType.RELATIONSHIP, IdType.NODE );

        assertIdTypesToReuseDisallows( IdType.NODE, IdType.RELATIONSHIP, IdType.RELATIONSHIP_GROUP );
        assertIdTypesToReuseDisallows( IdType.SCHEMA, IdType.NEOSTORE_BLOCK );
    }

    @Test
    public void idTypesToReuseCaseInsensitive()
    {
        Config config1 = new Config( stringMap( idTypesToReuse.name(), "node, relationship" ) );
        assertEquals( asList( IdType.NODE, IdType.RELATIONSHIP ), config1.get( idTypesToReuse ) );

        Config config2 = new Config( stringMap( idTypesToReuse.name(), "rElAtIoNshiP, NoDe" ) );
        assertEquals( asList( IdType.RELATIONSHIP, IdType.NODE ), config2.get( idTypesToReuse ) );
    }

    private static void assertIdTypesToReuseAllows( IdType type, IdType... otherTypes )
    {
        Config config = configWithIdTypes( type, otherTypes );
        List<IdType> types = config.get( idTypesToReuse );
        assertEquals( asList( type, otherTypes ), types );
    }

    private static void assertIdTypesToReuseDisallows( IdType type, IdType... otherTypes )
    {
        Config config = configWithIdTypes( type, otherTypes );
        try
        {
            config.get( idTypesToReuse );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( InvalidSettingException.class ) );
        }
    }

    private static Config configWithIdTypes( IdType type, IdType... otherTypes )
    {
        String value = stringList( type, otherTypes );
        return new Config( stringMap( idTypesToReuse.name(), value ) );
    }

    @SafeVarargs
    private static <T> String stringList( T element, T... elements )
    {
        return StringUtils.join( asList( element, elements ), "," );
    }

    @SafeVarargs
    private static <T> List<T> asList( T element, T... elements )
    {
        List<T> list = new ArrayList<>();
        list.add( element );
        Collections.addAll( list, elements );
        return list;
    }
}
