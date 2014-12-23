/**
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
package org.neo4j.graphdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Lookup.endsWith;
import static org.neo4j.graphdb.Lookup.equalTo;
import static org.neo4j.graphdb.Lookup.greaterThan;
import static org.neo4j.graphdb.Lookup.greaterThanOrEqualTo;
import static org.neo4j.graphdb.Lookup.lessThan;
import static org.neo4j.graphdb.Lookup.lessThanOrEqualTo;
import static org.neo4j.graphdb.Lookup.not;
import static org.neo4j.graphdb.Lookup.startsWith;

public class LookupApiTest
{
    @Test
    public void shouldSimplify() throws Exception
    {
        assertEquals( equalTo( "foo" ), not( not( equalTo( "foo" ) ) ) );
        assertEquals( not( lessThan( 17 ) ), greaterThanOrEqualTo( 17 ) );
        assertEquals( not( greaterThanOrEqualTo( 62 ) ), lessThan( 62 ) );
        assertEquals( not( greaterThan( 28 ) ), lessThanOrEqualTo( 28 ) );
        assertEquals( not( lessThanOrEqualTo( -2 ) ), greaterThan( -2 ) );
        assertEquals( equalTo( 19 ), lessThanOrEqualTo( 19 ).andGreaterThanOrEqualTo( 19 ) );
        assertEquals( equalTo( 35 ), greaterThanOrEqualTo( 35 ).andLessThanOrEqualTo( 35 ) );
    }

    @Test
    public void shouldRejectInvalidRanges() throws Exception
    {
        // when
        try { lessThan( 14 ).andGreaterThan( 14 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { lessThan( 31 ).andGreaterThanOrEqualTo( 31 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { lessThanOrEqualTo( 64 ).andGreaterThan( 64 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}

        // when
        try { greaterThan( 12 ).andLessThan( 12 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { greaterThan( 314 ).andLessThanOrEqualTo( 314 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { greaterThanOrEqualTo( 99 ).andLessThan( 99 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}

        // when
        try { lessThan( 10 ).andGreaterThan( 20 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { lessThan( 10 ).andGreaterThanOrEqualTo( 20 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}

        // when
        try { lessThanOrEqualTo( 20 ).andGreaterThan( 30 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { lessThanOrEqualTo( 20 ).andGreaterThanOrEqualTo( 30 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}

        // when
        try { greaterThan( 50 ).andLessThan( 40 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { greaterThan( 50 ).andLessThanOrEqualTo( 40 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}

        // when
        try { greaterThanOrEqualTo( 70 ).andLessThan( 60 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
        // when
        try { greaterThanOrEqualTo( 70 ).andLessThanOrEqualTo( 60 );
        // then
        fail( "expected exception" ); } catch ( IllegalArgumentException e ) {/* ok */}
    }

    @Test
    public void shouldHaveSensibleStringRepresentations() throws Exception
    {
        assertEquals( "equalTo(something)", equalTo( "something" ).toString() );
        assertEquals( "equalTo(double negation)", not( not( equalTo( "double negation" ) ) ).toString() );
        assertEquals( "startsWith(a prefix)", startsWith( "a prefix" ).toString() );
        assertEquals( "endsWith(some suffix)", endsWith( "some suffix" ).toString() );
        assertEquals( "lessThan(64)", lessThan( 64 ).toString() );
        assertEquals( "lessThanOrEqualTo(128)", lessThanOrEqualTo( 128 ).toString() );
        assertEquals( "greaterThan(17)", greaterThan( 17 ).toString() );
        assertEquals( "greaterThanOrEqualTo(42)", greaterThanOrEqualTo( 42 ).toString() );
        assertEquals( "not(startsWith(my name))", not( startsWith( "my name" ) ).toString() );
        assertEquals( "greaterThan(17).andLessThan(32)", greaterThan( 17 ).andLessThan( 32 ).toString() );
        assertEquals( "greaterThan(17).andLessThanOrEqualTo(32)",
                      greaterThan( 17 ).andLessThanOrEqualTo( 32 ).toString() );
        assertEquals( "greaterThanOrEqualTo(17).andLessThan(32)",
                      greaterThanOrEqualTo( 17 ).andLessThan( 32 ).toString() );
        assertEquals( "greaterThanOrEqualTo(17).andLessThanOrEqualTo(32)", greaterThanOrEqualTo(
                17 ).andLessThanOrEqualTo( 32 ).toString() );
    }
}
