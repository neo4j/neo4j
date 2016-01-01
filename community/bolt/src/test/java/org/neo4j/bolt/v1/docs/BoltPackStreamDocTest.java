/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.docs;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@RunWith( Parameterized.class )
public class BoltPackStreamDocTest
{
    @Parameterized.Parameter( 0 )
    public DocSerializationExample example;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedPackstreamExamples()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocSerializationExample ex : DocsRepository.docs().read(
                "dev/serialization.asciidoc",
                "code[data-lang=\"bolt_packstream_type\"]",
                DocSerializationExample.serialization_example ) )
        {
            mappings.add( new Object[]{ex} );
        }

        for ( DocSerializationExample ex : DocsRepository.docs().read(
                "dev/messaging.asciidoc",
                "code[data-lang=\"bolt_packstream_type\"]",
                DocSerializationExample.serialization_example ) )
        {
            mappings.add( new Object[]{ex} );
        }

        return mappings;
    }

    @Test
    public void serializingLeadsToSpecifiedOutput() throws Throwable
    {
        assertThat( "Serialized version of value should match documented data: " + example,
                DocSerialization.normalizedHex( example.serializedData() ),
                equalTo( DocSerialization.normalizedHex( DocSerialization.pack( example.attribute( "Value" ) ) ) ) );
    }
}
