/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.input.csv;

import org.junit.Test;

import java.io.StringReader;
import java.util.Map;

import org.neo4j.csv.reader.CharReadable;
import org.neo4j.csv.reader.Readables;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.UpdateBehaviour;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration.Overriden;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.unsafe.impl.batchimport.input.Collectors.silentBadCollector;
import static org.neo4j.unsafe.impl.batchimport.input.InputEntityDecorators.NO_NODE_DECORATOR;
import static org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;

public class ExternalPropertiesDecoratorTest
{
    @Test
    public void shouldCombineNodesWithExternalPropertiesFile() throws Exception
    {
        // GIVEN
        String propertyData =
                ":ID,email:string\n" +
                "1,mattias@some.com\n" +
                "1,mattiasp@someother.com\n" +
                "3,chris@abc\n" +
                "4,dude@yo";
        Configuration config = config();
        IdType idType = IdType.STRING;
        Function<InputNode,InputNode> externalPropertiesDecorator = new ExternalPropertiesDecorator(
                DataFactories.<InputNode>data( NO_NODE_DECORATOR, readable( propertyData ) ),
                defaultFormatNodeFileHeader(), config, idType, UpdateBehaviour.ADD,
                silentBadCollector( 0 ));

        // WHEN
        assertProperties( externalPropertiesDecorator.apply( node( "1", "key", "value1" ) ),
                "key", "value1", "email", new String[] {"mattias@some.com", "mattiasp@someother.com" } );
        // simulate there being a node in between here that has no corresponding external property
        assertProperties( externalPropertiesDecorator.apply( node( "2", "key", "value2" ) ),
                "key", "value2" );
        assertProperties( externalPropertiesDecorator.apply( node( "3", "key", "value3" ) ),
                "key", "value3", "email", "chris@abc" );
        assertProperties( externalPropertiesDecorator.apply( node( "4", "key", "value4" ) ),
                "key", "value4", "email", "dude@yo" );
    }

    @Test
    public void shouldCombineNodesWithExternalPropertyArraysFile() throws Exception
    {
        // GIVEN
        String propertyData =
                ":ID,email:string[]\n" +
                "1,mattias@some.com;mattiasp@someother.com\n" +
                "3,chris@abc\n" +
                "4,dude@yo";
        Configuration config = config();
        IdType idType = IdType.STRING;
        Function<InputNode,InputNode> externalPropertiesDecorator = new ExternalPropertiesDecorator(
                DataFactories.<InputNode>data( NO_NODE_DECORATOR, readable( propertyData ) ),
                defaultFormatNodeFileHeader(), config, idType, UpdateBehaviour.ADD,
                silentBadCollector( 0 ));

        // WHEN
        assertProperties( externalPropertiesDecorator.apply( node( "1", "key", "value1", "email", "existing" ) ),
                "key", "value1", "email", new String[] {"existing", "mattias@some.com", "mattiasp@someother.com" } );
        // simulate there being a node in between here that has no corresponding external property
        assertProperties( externalPropertiesDecorator.apply( node( "2", "key", "value2" ) ),
                "key", "value2" );
        assertProperties( externalPropertiesDecorator.apply( node( "3", "key", "value3" ) ),
                "key", "value3", "email", new String[] {"chris@abc"} );
        assertProperties( externalPropertiesDecorator.apply( node( "4", "key", "value4" ) ),
                "key", "value4", "email", new String[] {"dude@yo"} );
    }

    private void assertProperties( InputNode decoratedNode, Object... expectedKeyValuePairs )
    {
        Map<String,Object> expectedProperties = map( expectedKeyValuePairs );
        Map<String,Object> properties = map( decoratedNode.properties() );
        assertEquals( properties.toString(), expectedProperties.size(), properties.size() );
        for ( Map.Entry<String,Object> expectedProperty : expectedProperties.entrySet() )
        {
            Object value = properties.get( expectedProperty.getKey() );
            assertNotNull( value );
            assertEquals( expectedProperty.getValue().getClass(), value.getClass() );
            if ( value.getClass().isArray() )
            {
                assertArrayEquals( (Object[]) expectedProperty.getValue(), (Object[]) value );
            }
            else
            {
                assertEquals( expectedProperty.getValue(), value );
            }
        }
    }

    private InputNode node( Object id, Object... props )
    {
        return new InputNode( "source", 1, 0, id, props, null, InputEntity.NO_LABELS, null );
    }

    private Supplier<CharReadable> readable( final String data )
    {
        return new Supplier<CharReadable>()
        {
            @Override
            public CharReadable get()
            {
                return Readables.wrap( new StringReader( data ) );
            }
        };
    }

    private Overriden config()
    {
        return new Configuration.Overriden( Configuration.COMMAS )
        {
            @Override
            public int bufferSize()
            {
                return 1_000;
            }
        };
    }
}
