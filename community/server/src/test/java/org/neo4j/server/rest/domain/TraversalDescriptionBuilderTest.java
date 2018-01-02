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
package org.neo4j.server.rest.domain;

import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.junit.Test;

public class TraversalDescriptionBuilderTest
{

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentOnUnknownExpanderType() throws Exception
    {
        // Given
        TraversalDescriptionBuilder builder = new TraversalDescriptionBuilder( true );
        Collection<Map<String,Object>> rels = new ArrayList<Map<String, Object>>();
        rels.add( map( "type", "blah" ) );

        // When
        builder.from( map(
                "relationships", rels,
                "expander", "Suddenly, a string!" ) );
    }

    @Test(expected = IllegalArgumentException.class)
    public void throwsIllegalArgumentOnNonStringExpanderType() throws Exception
    {
        // Given
        TraversalDescriptionBuilder builder = new TraversalDescriptionBuilder( true );
        Collection<Map<String,Object>> rels = new ArrayList<Map<String, Object>>();
        rels.add( map( "type", "blah" ) );

        // When
        builder.from( map(
                "relationships", rels,
                "expander", map( ) ) );
    }

}
