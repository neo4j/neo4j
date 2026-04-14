/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport.input.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.batchimport.api.input.IdType;

/**
 * @author Gerrit Meier
 */
class ParquetColumnTest {

    @ParameterizedTest
    @CsvSource(
            value = {
                // column definitions, no configuration
                "id,",
                "id:int,",
                "id:int[],",
                "id:point{crs:WGS-84},{crs:WGS-84}",
                "id:point[]{crs:WGS-84},{crs:WGS-84}",
                ":ID,",
                "id:ID,",
                ":ID(id-space),",
                "id:ID(id-space),",
                // column definitions, with configuration
                ":ID{id-type: long},{id-type: long}",
                "id:ID{id-type: long},{id-type: long}",
                ":ID(id-space){id-type: long},{id-type: long}",
                "id:ID(id-space){id-type: long},{id-type: long}",
                "id:ID(id-space){this_is: 'a weird {name}'},{this_is: 'a weird {name}'}",
                "id:ID(id-space):ID(n@0<p@0_0>){id-type:long},{id-type:long}", // thanks to automated tools
                // same as before, with whitespace variations
                ":ID  {  id-type: long     }  ,{id-type: long}",
                "id:ID  {  id-type: long     }  ,{id-type: long}",
                ":ID(id-space)  {  id-type: long     }  ,{id-type: long}",
                ":ID    (    id-space   )  {  id-type: long     }  ,{id-type: long}",
                "id:ID(id-space)  {  id-type: long     }  ,{id-type: long}",
                "id:ID    (    id-space   )  {  id-type: long     }  ,{id-type: long}",
                "id:ID(id-space)  {   this_is: 'a weird {name}' },  {this_is: 'a weird {name}'}",
                "id:ID    (    id-space   )    {   this_is: 'a weird {name}' }  ,{this_is: 'a weird {name}'}",
            })
    void parsesColumnConfiguration(String input, String expected) {
        var column = ParquetColumn.from(
                ParquetColumn.HeaderDefinition.from(input),
                EntityType.NODE,
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "name"),
                LogicalTypeAnnotation.intType(32));

        assertThat(column.rawConfiguration()).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("neo4jTypes")
    void supportsAllNeo4jTypes(String rawType, ParquetColumnType expectedType) {
        var header = "prop%s".formatted(rawType == null ? "" : ":" + rawType);

        var column = ParquetColumn.from(
                ParquetColumn.HeaderDefinition.from(header),
                EntityType.RELATIONSHIP,
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "name"),
                LogicalTypeAnnotation.intType(32));

        assertThat(column.columnType()).isEqualTo(expectedType);
    }

    @Test
    void supportsIdTypeConversion() {
        // Given an id property with an id space type setting, with a different logical type
        String header = "id:ID(uniq_id){id-type: string}";

        // When parsing the column definition
        var column = ParquetColumn.from(
                ParquetColumn.HeaderDefinition.from(header),
                EntityType.NODE,
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "name"),
                LogicalTypeAnnotation.intType(32));

        // Then the column type is still ID, as the id space type sets the column type
        assertThat(column.columnType()).isEqualTo(ParquetColumnType.STRING);
        assertThat(column.columnIdType()).isEqualTo(IdType.STRING);
        assertThat(column.isIdColumn()).isTrue();
    }

    @Test
    void doesNotConvertActualIdTypes() {
        // Given an id property with an id space type setting, with a different logical type
        String header = "id:ID(uniq_id){id-type: actual}";

        // When parsing the column definition
        var column = ParquetColumn.from(
                ParquetColumn.HeaderDefinition.from(header),
                EntityType.NODE,
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "name"),
                LogicalTypeAnnotation.intType(32));

        // Then the column type is still ID, as the id space type sets the column type
        assertThat(column.columnType()).isEqualTo(ParquetColumnType.RAW);
        assertThat(column.columnIdType()).isEqualTo(IdType.ACTUAL);
        assertThat(column.isIdColumn()).isTrue();
    }

    private static Stream<Arguments> neo4jTypes() {
        return Stream.of(
                Arguments.of(null, ParquetColumnType.RAW),
                Arguments.of("boolean", ParquetColumnType.BOOLEAN),
                Arguments.of("BOOLEAN[]", ParquetColumnType.BOOLEAN),
                Arguments.of("point", ParquetColumnType.POINT),
                Arguments.of("POINT[]", ParquetColumnType.POINT),
                Arguments.of("date", ParquetColumnType.DATE),
                Arguments.of("DATE[]", ParquetColumnType.DATE),
                Arguments.of("time", ParquetColumnType.TIME),
                Arguments.of("TIME[]", ParquetColumnType.TIME),
                Arguments.of("datetime", ParquetColumnType.DATE_TIME),
                Arguments.of("DATETIME[]", ParquetColumnType.DATE_TIME),
                Arguments.of("localtime", ParquetColumnType.LOCAL_TIME),
                Arguments.of("LOCALTIME[]", ParquetColumnType.LOCAL_TIME),
                Arguments.of("localdatetime", ParquetColumnType.LOCAL_DATE_TIME),
                Arguments.of("LOCALDATETIME[]", ParquetColumnType.LOCAL_DATE_TIME),
                Arguments.of("duration", ParquetColumnType.DURATION),
                Arguments.of("DURATION[]", ParquetColumnType.DURATION),
                Arguments.of("string", ParquetColumnType.STRING),
                Arguments.of("STRING[]", ParquetColumnType.STRING),
                Arguments.of("long", ParquetColumnType.LONG),
                Arguments.of("LONG[]", ParquetColumnType.LONG),
                Arguments.of("double", ParquetColumnType.DOUBLE),
                Arguments.of("DOUBLE[]", ParquetColumnType.DOUBLE),
                Arguments.of("int", ParquetColumnType.INT),
                Arguments.of("INT[]", ParquetColumnType.INT),
                Arguments.of("float", ParquetColumnType.FLOAT),
                Arguments.of("FLOAT[]", ParquetColumnType.FLOAT),
                Arguments.of("byte", ParquetColumnType.BYTE),
                Arguments.of("BYTE[]", ParquetColumnType.BYTE),
                Arguments.of("short", ParquetColumnType.SHORT),
                Arguments.of("SHORT[]", ParquetColumnType.SHORT),
                Arguments.of("char", ParquetColumnType.CHAR),
                Arguments.of("CHAR[]", ParquetColumnType.CHAR));
    }
}
