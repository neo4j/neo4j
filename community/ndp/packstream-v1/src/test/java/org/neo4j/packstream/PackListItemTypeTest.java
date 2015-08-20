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
package org.neo4j.packstream;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class PackListItemTypeTest
{

    @Test
    public void javaObjectClassMapsToPackStreamAny() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Object.class );

        // Then
        assertThat( type, equalTo( PackListItemType.ANY ) );

    }

    @Test
    public void javaBooleanClassMapsToPackStreamBoolean() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Boolean.class );

        // Then
        assertThat( type, equalTo( PackListItemType.BOOLEAN ) );

    }

    @Test
    public void javaShortClassMapsToPackStreamInteger() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Short.class );

        // Then
        assertThat( type, equalTo( PackListItemType.INTEGER ) );

    }

    @Test
    public void javaIntegerClassMapsToPackStreamInteger() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Integer.class );

        // Then
        assertThat( type, equalTo( PackListItemType.INTEGER ) );

    }

    @Test
    public void javaLongClassMapsToPackStreamInteger() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Long.class );

        // Then
        assertThat( type, equalTo( PackListItemType.INTEGER ) );

    }

    @Test
    public void javaDoubleClassMapsToPackStreamFloat() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Double.class );

        // Then
        assertThat( type, equalTo( PackListItemType.FLOAT ) );

    }

    @Test
    public void javaByteArrayClassMapsToPackStreamText() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( byte[].class );

        // Then
        assertThat( type, equalTo( PackListItemType.BYTES ) );

    }

    @Test
    public void javaStringClassMapsToPackStreamText() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( String.class );

        // Then
        assertThat( type, equalTo( PackListItemType.TEXT ) );

    }

    @Test
    public void javaListClassMapsToPackStreamList() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( List.class );

        // Then
        assertThat( type, equalTo( PackListItemType.LIST ) );

    }

    @Test
    public void javaMapClassMapsToPackStreamMap() throws Throwable
    {
        // Given
        PackListItemType type = PackListItemType.fromClass( Map.class );

        // Then
        assertThat( type, equalTo( PackListItemType.MAP ) );

    }

}
