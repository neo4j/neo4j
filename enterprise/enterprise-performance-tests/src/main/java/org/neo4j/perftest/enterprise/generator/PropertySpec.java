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
package org.neo4j.perftest.enterprise.generator;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.perftest.enterprise.util.Conversion;

class PropertySpec
{
    public static final Conversion<String,PropertySpec> PARSER = new Conversion<String, PropertySpec>()
    {
        @Override
        public PropertySpec convert( String value )
        {
            String[] tokens = value.split( ":" );
            return new PropertySpec( PropertyGenerator.valueOf( tokens[0] ), Float.parseFloat( tokens[1] ) );
        }
    };

    private final PropertyGenerator propertyGenerator;
    private final float count;

    PropertySpec( PropertyGenerator propertyGenerator, float count )
    {
        this.propertyGenerator = propertyGenerator;
        this.count = count;
    }

    public Map<String, Object> generate()
    {
        HashMap<String, Object> map = new HashMap<String, Object>();
        int propertyCount = (int) count;
        if ( DataGenerator.RANDOM.nextFloat() < count - propertyCount)
        {
            propertyCount++;
        }
        for ( int i = 0; i < propertyCount; i++ )
        {
            map.put( propertyGenerator.name() + "_" + i, propertyGenerator.generate() );
        }
        return map;
    }

    @Override
    public String toString()
    {
        return propertyGenerator.name() + ":" + count;
    }
}
