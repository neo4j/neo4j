/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.exceptions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.IndexHintException.IndexHintIndexType;

import static org.junit.jupiter.api.Assertions.assertEquals;

class IndexHintExceptionTest
{
    @Test
    void indexFormatStringForSingleNodePropertyIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR (`person`:`Person`) ON (`person`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringForCompositeNodePropertyIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Arrays.asList( "name", "surname" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR (`person`:`Person`) ON (`person`.`name`, `person`.`surname`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringForSingleRelationshipPropertyIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.RELATIONSHIP,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringForCompositeRelationshipPropertyIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Arrays.asList( "name", "surname" ),
                                                              EntityType.RELATIONSHIP,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR ()-[`person`:`Person`]-() ON (`person`.`name`, `person`.`surname`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringEscapesVariableName()
    {
        String actual = IndexHintException.indexFormatString( "pers`on",
                                                              "Person",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR (`pers``on`:`Person`) ON (`pers``on`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringEscapesLabelName()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Pers`on",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR (`person`:`Pers``on`) ON (`person`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringEscapesRelationshipTypeName()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Pers`on",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.RELATIONSHIP,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR ()-[`person`:`Pers``on`]-() ON (`person`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringEscapesPropertyNames()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Arrays.asList( "nam`e", "s`urname" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.ANY );
        String expected = "INDEX FOR (`person`:`Person`) ON (`person`.`nam``e`, `person`.`s``urname`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringForBtreeIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.BTREE );
        String expected = "BTREE INDEX FOR (`person`:`Person`) ON (`person`.`name`)";
        assertEquals( expected, actual );
    }

    @Test
    void indexFormatStringForTextIndex()
    {
        String actual = IndexHintException.indexFormatString( "person",
                                                              "Person",
                                                              Collections.singletonList( "name" ),
                                                              EntityType.NODE,
                                                              IndexHintIndexType.TEXT );
        String expected = "TEXT INDEX FOR (`person`:`Person`) ON (`person`.`name`)";
        assertEquals( expected, actual );
    }
}
