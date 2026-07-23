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
package org.neo4j.internal.batchimport.input.csv;

import static java.nio.charset.Charset.defaultCharset;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.csv.reader.Readables.wrap;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatNodeFileHeader;
import static org.neo4j.internal.batchimport.input.csv.DataFactories.defaultFormatRelationshipFileHeader;
import static org.neo4j.internal.helpers.ArrayUtil.array;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.Function;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractor;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.MultiReadable;
import org.neo4j.internal.batchimport.input.DuplicateHeaderException;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.CSVHeaderInformation;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;

@TestDirectoryExtension
class DataFactoriesTest {
    private static final int BUFFER_SIZE = 10_000;
    private static final Configuration COMMAS =
            Configuration.COMMAS.toBuilder().withBufferSize(BUFFER_SIZE).build();
    private static final Configuration TABS =
            Configuration.TABS.toBuilder().withBufferSize(BUFFER_SIZE).build();

    @Inject
    private TestDirectory directory;

    private final Groups groups = new Groups();
    private final Group globalGroup = groups.getOrCreate(null);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldParseRawHeaderCorrectly(boolean commasOrTabs) throws IOException {
        var config = commasOrTabs ? COMMAS : TABS;
        var headers = Lists.mutable.of(
                "ID:ID", "label-one:label", "also-labels:LABEL", "name", "age:long", "location:Point{crs:WGS-84}");
        var rawHeaders = DataFactories.parseRawHeaderEntries(
                "",
                config,
                () -> ZoneOffset.UTC,
                String.join(commasOrTabs ? "," : "\t", headers).toCharArray());
        assertThat(rawHeaders).containsExactlyElementsOf(headers);
    }

    @Test
    void shouldParseDefaultNodeFileHeaderCorrectly() throws Exception {
        // GIVEN
        CharSeeker seeker = seeker("ID:ID,label-one:label,also-labels:LABEL,name,age:long,location:Point{crs:WGS-84}");
        IdType idType = IdType.STRING;
        Extractors extractors = new Extractors(',', ',');

        // WHEN
        Header header = defaultFormatNodeFileHeader().create(seeker, COMMAS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry("ID", Type.ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry("label-one", Type.LABEL, extractors.stringArray()),
                        entry("also-labels", Type.LABEL, extractors.stringArray()),
                        entry("name", Type.PROPERTY, extractors.string()),
                        entry("age", Type.PROPERTY, extractors.long_()),
                        propertyEntry(
                                "location",
                                extractors.point(),
                                Map.of("crs", "WGS-84"),
                                PointValue.parseHeaderInformation("{crs:WGS-84}"))));
        seeker.close();
    }

    @Test
    void shouldParseNodeArrayTypesHeaderCorrectly() throws Exception {
        // GIVEN
        CharSeeker seeker =
                seeker("ID:ID,longArray:long[],pointArray:Point[]{crs:WGS-84},timeArray:time[]{timezone:+02:00},"
                        + "dateTimeArray:datetime[]{timezone:+02:00}");
        IdType idType = IdType.STRING;
        Extractors extractors = new Extractors(',', ',');

        // WHEN
        Header header = defaultFormatNodeFileHeader().create(seeker, COMMAS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry("ID", Type.ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry("longArray", Type.PROPERTY, extractors.longArray()),
                        propertyEntry(
                                "pointArray",
                                extractors.pointArray(),
                                Map.of("crs", "WGS-84"),
                                PointValue.parseHeaderInformation("{crs:WGS-84}")),
                        propertyEntry(
                                "timeArray",
                                extractors.timeArray(),
                                Map.of("timezone", "+02:00"),
                                TimeValue.parseHeaderInformation("{timezone:+02:00}")),
                        propertyEntry(
                                "dateTimeArray",
                                extractors.dateTimeArray(),
                                Map.of("timezone", "+02:00"),
                                DateTimeValue.parseHeaderInformation("{timezone:+02:00}"))));
        seeker.close();
    }

    @Test
    void shouldParseDefaultRelationshipFileHeaderCorrectly() throws Exception {
        // GIVEN
        CharSeeker seeker = seeker(":START_ID\t:END_ID\ttype:TYPE\tdate:long\tmore:long[]");
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors('\t', '\t');

        // WHEN
        Header header = defaultFormatRelationshipFileHeader().create(seeker, TABS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry(null, Type.START_ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry(null, Type.END_ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry("type", Type.TYPE, extractors.string()),
                        entry("date", Type.PROPERTY, extractors.long_()),
                        entry("more", Type.PROPERTY, extractors.longArray())));
        seeker.close();
    }

    @Test
    void shouldParseRelationshipArrayTypesFileHeaderCorrectly() throws Exception {
        // GIVEN
        CharSeeker seeker = seeker(":START_ID\t:END_ID\ttype:TYPE\tlongArray:long[]\tpointArray:Point[]{crs:WGS-84}"
                + "\ttimeArray:time[]{timezone:+02:00}\tdateTimeArray:datetime[]{timezone:+02:00}");
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors('\t', '\t');

        // WHEN
        Header header = defaultFormatRelationshipFileHeader().create(seeker, TABS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry(null, Type.START_ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry(null, Type.END_ID, globalGroup, CsvInput.idExtractor(idType, extractors)),
                        entry("type", Type.TYPE, extractors.string()),
                        entry("longArray", Type.PROPERTY, extractors.longArray()),
                        propertyEntry(
                                "pointArray",
                                extractors.pointArray(),
                                Map.of("crs", "WGS-84"),
                                PointValue.parseHeaderInformation("{crs:WGS-84}")),
                        propertyEntry(
                                "timeArray",
                                extractors.timeArray(),
                                Map.of("timezone", "+02:00"),
                                TimeValue.parseHeaderInformation("{timezone:+02:00}")),
                        propertyEntry(
                                "dateTimeArray",
                                extractors.dateTimeArray(),
                                Map.of("timezone", "+02:00"),
                                DateTimeValue.parseHeaderInformation("{timezone:+02:00}"))));
        seeker.close();
    }

    @Test
    void shouldHaveEmptyHeadersBeInterpretedAsIgnored() throws Exception {
        // GIVEN
        CharSeeker seeker = seeker("one:id\ttwo\t\tdate:long");
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors('\t', '\t');

        // WHEN
        Header header = defaultFormatNodeFileHeader().create(seeker, TABS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry("one", Type.ID, globalGroup, extractors.long_()),
                        entry("two", Type.PROPERTY, extractors.string()),
                        entry(null, Type.IGNORE, null),
                        entry("date", Type.PROPERTY, extractors.long_())));
        seeker.close();
    }

    @Test
    void shouldFailForDuplicatePropertyHeaderEntries() throws Exception {
        // GIVEN
        var extractors = new Extractors('\t', '\t');
        try (var seeker = seeker("one:id\tname\tname:long")) {
            assertThatThrownBy(() -> defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups))
                    .isInstanceOf(DuplicateHeaderException.class)
                    .satisfies(e -> {
                        var headerEx = (DuplicateHeaderException) e;
                        assertThat(headerEx.getFirst()).isEqualTo(entry("name", Type.PROPERTY, extractors.string()));
                        assertThat(headerEx.getOther()).isEqualTo(entry("name", Type.PROPERTY, extractors.long_()));
                    });
        }
    }

    @Test
    void shouldFailForDuplicatePropertyAndNamedIdHeaderEntries() throws Exception {
        // GIVEN
        var extractors = new Extractors('\t', '\t');
        try (var seeker = seeker("one:id\tone")) {
            assertThatThrownBy(() -> defaultFormatNodeFileHeader().create(seeker, TABS, IdType.STRING, groups))
                    .isInstanceOf(DuplicateHeaderException.class)
                    .satisfies(e -> {
                        var headerEx = (DuplicateHeaderException) e;
                        assertThat(headerEx.getFirst())
                                .isEqualTo(entry("one", Type.ID, globalGroup, extractors.string()));
                        assertThat(headerEx.getOther()).isEqualTo(entry("one", Type.PROPERTY, extractors.string()));
                    });
        }
    }

    @Test
    void shouldHandleDuplicateIdHeaderEntries() {
        // GIVEN
        CharSeeker seeker = seeker("one:id\ttwo:id");
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors('\t', '\t');

        var header = defaultFormatNodeFileHeader().create(seeker, TABS, idType, groups);
        assertThat(header.entries())
                .containsExactly(array(
                        entry("one", Type.ID, globalGroup, extractors.long_()),
                        entry("two", Type.ID, globalGroup, extractors.long_())));
    }

    @Test
    void shouldAllowMissingIdHeaderEntry() throws Exception {
        // GIVEN
        CharSeeker seeker = seeker("one\ttwo");
        Extractors extractors = new Extractors();

        // WHEN
        Header header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry("one", Type.PROPERTY, extractors.string()),
                        entry("two", Type.PROPERTY, extractors.string())));
        seeker.close();
    }

    @Test
    void shouldParseHeaderFromFirstLineOfFirstInputFile() throws Exception {
        // GIVEN
        Path headerFile = writeFile("header", "id:ID\tname:String\tbirth_date:long");
        Path dataFile = writeFile("data", "0\tThe node\t123456789");
        DataFactory dataFactory = DataFactories.data(value -> value, defaultCharset(), headerFile, dataFile);
        Header.Factory headerFactory = defaultFormatNodeFileHeader();
        Extractors extractors = new Extractors();

        // WHEN
        CharSeeker seeker = CharSeekers.charSeeker(new MultiReadable(dataFactory.create(TABS).stream()), TABS, false);
        Header header = headerFactory.create(seeker, TABS, IdType.ACTUAL, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry("id", Type.ID, globalGroup, extractors.long_()),
                        entry("name", Type.PROPERTY, extractors.string()),
                        entry("birth_date", Type.PROPERTY, extractors.long_())));
        seeker.close();
    }

    @Test
    void shouldParseGroupName() throws Exception {
        // GIVEN
        String groupOneName = "GroupOne";
        String groupTwoName = "GroupTwo";
        CharSeeker seeker = seeker(
                ":START_ID(" + groupOneName + ")\t:END_ID(" + groupTwoName + ")\ttype:TYPE\tdate:long\tmore:long[]");
        IdType idType = IdType.ACTUAL;
        Extractors extractors = new Extractors('\t', '\t');
        groups.getOrCreate(groupOneName);
        groups.getOrCreate(groupTwoName);

        // WHEN
        Header header = defaultFormatRelationshipFileHeader().create(seeker, TABS, idType, groups);

        // THEN
        assertThat(header.entries())
                .containsExactly(array(
                        entry(null, Type.START_ID, groups.get("GroupOne"), CsvInput.idExtractor(idType, extractors)),
                        entry(null, Type.END_ID, groups.get("GroupTwo"), CsvInput.idExtractor(idType, extractors)),
                        entry("type", Type.TYPE, extractors.string()),
                        entry("date", Type.PROPERTY, extractors.long_()),
                        entry("more", Type.PROPERTY, extractors.longArray())));
        seeker.close();
    }

    @Test
    void shouldFailOnUnexpectedNodeHeaderType() throws Exception {
        // GIVEN
        try (var seeker = seeker(":ID,:START_ID")) {
            IdType idType = IdType.ACTUAL;

            // WHEN
            assertThatThrownBy(() -> defaultFormatNodeFileHeader().create(seeker, COMMAS, idType, groups))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("START_ID");
        }
    }

    @Test
    void shouldFailOnUnexpectedRelationshipHeaderType() throws Exception {
        // GIVEN
        try (var seeker = seeker(":LABEL,:START_ID,:END_ID,:TYPE")) {
            IdType idType = IdType.ACTUAL;

            // WHEN
            assertThatThrownBy(() -> defaultFormatRelationshipFileHeader().create(seeker, COMMAS, idType, groups))
                    .isInstanceOf(InputException.class)
                    .hasMessageContaining("LABEL");
        }
    }

    @Test
    void shouldParseHeaderOptionsMap() {
        // GIVEN
        var seeker = seeker("id:ID(myGroup){myFirstKey:10,mySecondKey:\"Some string\"}\t:LABEL");

        // WHEN
        var header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups);

        // THEN
        var extractors = new Extractors();
        assertThat(header.entries())
                .isEqualTo(array(
                        idEntry(
                                "id",
                                groups.get("myGroup"),
                                extractors.long_(),
                                Map.of("myFirstKey", "10", "mySecondKey", "Some string")),
                        entry(null, Type.LABEL, extractors.stringArray())));
    }

    @Test
    void shouldParseHeaderOptionsMapEvenWhenItIsBeforeTheGroup() {
        // GIVEN
        var seeker = seeker("id:ID{myFirstKey:10,mySecondKey:\"Some string\"}(myGroup)\t:LABEL");

        // WHEN
        var header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups);

        // THEN
        var extractors = new Extractors();
        assertThat(header.entries())
                .isEqualTo(array(
                        idEntry(
                                "id",
                                groups.get("myGroup"),
                                extractors.long_(),
                                Map.of("myFirstKey", "10", "mySecondKey", "Some string")),
                        entry(null, Type.LABEL, extractors.stringArray())));
    }

    @Test
    void shouldParseHeaderOptionsMapWithoutGroup() {
        // GIVEN
        var seeker = seeker("id:ID{myFirstKey:10,mySecondKey:\"Some string\"}\t:LABEL");

        // WHEN
        var header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups);

        // THEN
        var extractors = new Extractors();
        assertThat(header.entries())
                .isEqualTo(array(
                        idEntry(
                                "id",
                                globalGroup,
                                extractors.long_(),
                                Map.of("myFirstKey", "10", "mySecondKey", "Some string")),
                        entry(null, Type.LABEL, extractors.stringArray())));
    }

    @Test
    void shouldCreateGroupWithSpecificIdType() {
        // GIVEN
        var seeker = seeker("id:ID(MyGroup){id-type:long}");

        // WHEN
        var header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.STRING, groups);

        // THEN
        var extractors = new Extractors();
        var group = groups.get("MyGroup");
        assertThat(header.entries())
                .isEqualTo(array(idEntry("id", group, extractors.long_(), Map.of("id-type", "long"))));
        assertThat(groups.getSpecificIdType(group, 0)).isEqualTo("long");
    }

    @Test
    void shouldParseWithMultipleSpecificIdTypes() throws IOException {
        // GIVEN
        var extractors = new Extractors();
        try (var seeker = seeker("id1:ID(MyGroup){id-type:string},id2:ID(MyGroup){id-type:long}")) {
            // WHEN
            var header = defaultFormatNodeFileHeader().create(seeker, COMMAS, IdType.STRING, groups);
            var group = groups.get("MyGroup");

            // THEN
            assertThat(header.entries())
                    .isEqualTo(array(
                            idEntry("id1", group, extractors.string(), Map.of("id-type", "string")),
                            idEntry("id2", group, extractors.long_(), Map.of("id-type", "long"))));
            assertThat(groups.getSpecificIdType(group, 0)).isEqualTo("string");
            assertThat(groups.getSpecificIdType(group, 1)).isEqualTo("long");
        }
    }

    @Test
    void shouldParseWithMultipleIdColumnsWithSomeIdTypes() throws IOException {
        // GIVEN
        var extractors = new Extractors();
        try (var seeker = seeker("id1:ID(MyGroup),id2:ID(MyGroup){id-type:long}")) {
            // WHEN
            var header = defaultFormatNodeFileHeader().create(seeker, COMMAS, IdType.STRING, groups);
            var group = groups.get("MyGroup");

            // THEN
            assertThat(header.entries())
                    .isEqualTo(array(
                            idEntry("id1", group, extractors.string(), Map.of()),
                            idEntry("id2", group, extractors.long_(), Map.of("id-type", "long"))));
            assertThat(groups.getSpecificIdType(group, 0)).isNull();
            assertThat(groups.getSpecificIdType(group, 1)).isEqualTo("long");
        }
    }

    @Test
    void shouldParseRelationshipsWithCompositeKeysThatUseNodeIdTypeInformation() throws Exception {
        // GIVEN
        var extractors = new Extractors();

        try (var seeker = seeker("id1:ID(MyGroup){id-type:int},id2:ID(MyGroup){id-type:long}")) {
            defaultFormatNodeFileHeader().create(seeker, COMMAS, IdType.STRING, groups);
        }

        try (var seeker = seeker(":START_ID(MyGroup),:START_ID(MyGroup),:END_ID(MyGroup),:END_ID(MyGroup),type:TYPE")) {
            // WHEN
            var header = defaultFormatRelationshipFileHeader().create(seeker, COMMAS, IdType.STRING, groups);
            var group = groups.get("MyGroup");

            // THEN
            assertThat(header.entries())
                    .containsExactly(array(
                            entry(null, Type.START_ID, group, extractors.int_()),
                            entry(null, Type.START_ID, group, extractors.long_()),
                            entry(null, Type.END_ID, group, extractors.int_()),
                            entry(null, Type.END_ID, group, extractors.long_()),
                            entry("type", Type.TYPE, extractors.string())));
        }
    }

    @Test
    void relationshipShouldInheritNodeIdTypeRegardlessOfColumnPositionAcrossFiles() throws Exception {
        // GIVEN a node header declaring the id space's id-type ...
        var extractors = new Extractors();
        try (var seeker = seeker("id:ID(MyGroup){id-type:long}")) {
            defaultFormatNodeFileHeader().create(seeker, COMMAS, IdType.STRING, groups);
        }
        var group = groups.get("MyGroup");

        // ... and a single relationship header factory reused across multiple relationship files,
        // exactly as CsvInput does for a whole relationship input.
        var relationshipHeaderFactory = defaultFormatRelationshipFileHeader();

        // WHEN the first relationship file has :START_ID/:END_ID at columns 0 and 1
        try (var seeker = seeker(":START_ID(MyGroup),:END_ID(MyGroup),type:TYPE")) {
            var header = relationshipHeaderFactory.create(seeker, COMMAS, IdType.STRING, groups);

            // THEN the id columns inherit the node-declared long id-type (not the global STRING default)
            assertThat(header.entries())
                    .containsExactly(array(
                            entry(null, Type.START_ID, group, extractors.long_()),
                            entry(null, Type.END_ID, group, extractors.long_()),
                            entry("type", Type.TYPE, extractors.string())));
        }

        // WHEN a subsequent relationship file (parsed by the SAME factory) places :START_ID/:END_ID at
        // different column positions
        try (var seeker = seeker("type:TYPE,:START_ID(MyGroup),:END_ID(MyGroup)")) {
            var header = relationshipHeaderFactory.create(seeker, COMMAS, IdType.STRING, groups);

            // THEN the id columns still inherit the node-declared long id-type. Before the fix, the shifted
            // column positions caused getSpecificIdType(...) to return null and the extractor to fall back to
            // the global STRING default, mismatching the id space's long encoder.
            assertThat(header.entries())
                    .containsExactly(array(
                            entry("type", Type.TYPE, extractors.string()),
                            entry(null, Type.START_ID, group, extractors.long_()),
                            entry(null, Type.END_ID, group, extractors.long_())));
        }
    }

    @Test
    void shouldParsePropertyHeaderWithColonInName() {
        // GIVEN
        var seeker = seeker("uri:ID,http://example.com/property/name:string[]");

        // WHEN
        var header = defaultFormatNodeFileHeader().create(seeker, COMMAS, IdType.STRING, groups);

        // THEN
        var extractors = new Extractors();
        var entries = header.entries();
        assertThat(entries)
                .isEqualTo(array(
                        entry("uri", Type.ID, globalGroup, extractors.string()),
                        entry("http://example.com/property/name", Type.PROPERTY, extractors.stringArray())));
    }

    @Test
    void shouldParseRemoveLabelHeader() {
        // GIVEN
        var alt1 = seeker(":ID,:LABEL,:REMOVE_LABEL");
        var alt2 = seeker(":ID,:+LABEL,:-LABEL");

        // WHEN
        var header1 = defaultFormatNodeFileHeader().create(alt1, COMMAS, IdType.STRING, groups);
        var header2 = defaultFormatNodeFileHeader().create(alt2, COMMAS, IdType.STRING, groups);

        // THEN
        var extractors = new Extractors();
        assertThat(header1.entries())
                .isEqualTo(array(
                        entry(null, Type.ID, globalGroup, extractors.string()),
                        entry(null, Type.LABEL, extractors.stringArray()),
                        entry(null, Type.REMOVE_LABEL, extractors.stringArray())));
        assertThat(header2.entries())
                .isEqualTo(array(
                        entry(null, Type.ID, globalGroup, extractors.string()),
                        entry(null, Type.LABEL, extractors.stringArray()),
                        entry(null, Type.REMOVE_LABEL, extractors.stringArray())));
    }

    @Test
    void shouldParseColumnNamesWithParentheses() throws Exception {
        assertParsedHeader(
                "location (name):String\tlocation (detail):Point{crs:WGS-84}", extractors -> new Header.Entry[] {
                    entry("location (name)", Type.PROPERTY, extractors.string()),
                    propertyEntry(
                            "location (detail)",
                            extractors.point(),
                            Map.of("crs", "WGS-84"),
                            PointValue.parseHeaderInformation("{crs:WGS-84}"))
                });
        assertParsedHeader("location (name):ID(MyGroup1)", extractors ->
                new Header.Entry[] {entry("location (name)", Type.ID, groups.get("MyGroup1"), extractors.long_())});
        assertParsedHeader("location (detail):ID(MyGroup2){opt:foo}", extractors -> new Header.Entry[] {
            idEntry("location (detail)", groups.get("MyGroup2"), extractors.long_(), Map.of("opt", "foo"))
        });
        assertParsedHeader(
                "commentId:ID(Comment):ID(n;0<p;0_0>){id-type:long}\tcreationDate:datetime:datetime"
                        + "\tlocationIP:string:string\tbrowserUsed:string:string\tcontent:string:string\tlength:int:long",
                extractors -> new Header.Entry[] {
                    idEntry(
                            "commentId:ID(Comment)",
                            groups.get("n;0<p;0_0>"),
                            extractors.long_(),
                            Map.of("id-type", "long")),
                    entry("creationDate:datetime", Type.PROPERTY, extractors.dateTime()),
                    entry("locationIP:string", Type.PROPERTY, extractors.string()),
                    entry("browserUsed:string", Type.PROPERTY, extractors.string()),
                    entry("content:string", Type.PROPERTY, extractors.string()),
                    entry("length:int", Type.PROPERTY, extractors.long_()),
                });
        assertParsedHeader(
                "commentId:ID(Comment):ID(n@0<p@0_0>){id-type:long}\tcreationDate:datetime:datetime"
                        + "\tlocationIP:string:string\tbrowserUsed:string:string\tcontent:string:string\tlength:int:long",
                extractors -> new Header.Entry[] {
                    idEntry(
                            "commentId:ID(Comment)",
                            groups.get("n@0<p@0_0>"),
                            extractors.long_(),
                            Map.of("id-type", "long")),
                    entry("creationDate:datetime", Type.PROPERTY, extractors.dateTime()),
                    entry("locationIP:string", Type.PROPERTY, extractors.string()),
                    entry("browserUsed:string", Type.PROPERTY, extractors.string()),
                    entry("content:string", Type.PROPERTY, extractors.string()),
                    entry("length:int", Type.PROPERTY, extractors.long_()),
                });
    }

    private void assertParsedHeader(String headerString, Function<Extractors, Header.Entry[]> entries)
            throws IOException {
        // GIVEN
        try (CharSeeker seeker = seeker(headerString)) {
            Extractors extractors = new Extractors();

            // WHEN
            Header header = defaultFormatNodeFileHeader().create(seeker, TABS, IdType.ACTUAL, groups);

            // THEN
            Header.Entry[] apply = entries.apply(extractors);
            assertThat(header.entries()).isEqualTo(apply);
        }
    }

    private static final Configuration SEEKER_CONFIG =
            Configuration.TABS.toBuilder().withBufferSize(1000).build();

    private static CharSeeker seeker(String data) {
        return CharSeekers.charSeeker(wrap(data), SEEKER_CONFIG, false);
    }

    private Path writeFile(String name, String content) throws IOException {
        Path file = directory.file(name);
        Files.writeString(file, content, defaultCharset());
        return file;
    }

    private Header.Entry entry(String name, Type type, Extractor<?> extractor) {
        return entry(name, type, null, extractor);
    }

    private Header.Entry propertyEntry(
            String name,
            Extractor<?> extractor,
            Map<String, String> rawOptions,
            CSVHeaderInformation optionalParameter) {
        return new Header.Entry(null, name, Type.PROPERTY, null, extractor, rawOptions, optionalParameter);
    }

    private Header.Entry idEntry(String name, Group group, Extractor<?> extractor, Map<String, String> rawOptions) {
        return new Header.Entry(null, name, Type.ID, group, extractor, rawOptions, null);
    }

    private Header.Entry entry(String name, Type type, Group group, Extractor<?> extractor) {
        return new Header.Entry(name, type, group, extractor);
    }
}
