/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.docs.v1;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.ndp.docs.v1.DocSerialization.normalizedHex;
import static org.neo4j.ndp.docs.v1.DocSerialization.pack;
import static org.neo4j.ndp.docs.v1.DocSerializationExample.serialization_example;
import static org.neo4j.ndp.docs.v1.DocsRepository.docs;

@RunWith( Parameterized.class )
public class NDPPackStreamDocTest
{
    @Parameterized.Parameter( 0 )
    public DocSerializationExample example;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedPackstreamExamples()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        for ( DocSerializationExample ex : docs().read(
                "dev/serialization.asciidoc",
                "code[data-lang=\"ndp_packstream_type\"]",
                serialization_example ) )
        {
            mappings.add( new Object[]{ex} );
        }

        for ( DocSerializationExample ex : docs().read(
                "dev/messaging.asciidoc",
                "code[data-lang=\"ndp_packstream_type\"]",
                serialization_example ) )
        {
            mappings.add( new Object[]{ex} );
        }

        return mappings;
    }

    @Test
    public void serializingLeadsToSpecifiedOutput() throws Throwable
    {
        assertThat( "Serialized version of value should match documented data: " + example,
                normalizedHex( example.serializedData() ),
                equalTo( normalizedHex( pack( example.attribute( "Value" ) ) ) ) );
    }
}
