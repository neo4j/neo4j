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
package org.neo4j.kernel.impl.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_ARG;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_AUTH;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_FRAGMENT;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_PATH;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_QUERY;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_URI;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.ERR_URL;
import static org.neo4j.kernel.impl.security.FileURLAccessRuleTest.ValidationStatus.OK;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;

class FileURLAccessRuleTest {

    @Test
    void shouldThrowWhenFileAccessIsDisabled() throws Exception {
        final URL url = new URL("file:///dir/file.csv");
        final Config config = Config.defaults(GraphDatabaseSettings.allow_file_urls, false);
        var error = assertThrows(URLAccessValidationError.class, () -> URLAccessRules.fileAccess()
                .validate(config, url));
        assertThat(error.getMessage())
                .isEqualTo("configuration property 'dbms.security.allow_csv_import_from_file_urls' is false");
    }

    @ParameterizedTest
    @MethodSource({
        // business logic
        "queryStringIsNotAllowed",
        "fragmentIsNotAllowed",
        "authorityIsNotAllowed",
        "pathsThatResembleAuthorities",
        "pathsWhichAreEmpty",
        "pathsWithLeadingSlashes",
        "pathsWithTrailingSlashes",
        "pathsWithTraversal",

        // special characters
        "charactersReservedFromUriProtocol",
        "charactersReservedFromApplicationForms",
        "charactersFromUnicodeInUsAsciiRange",
        "charactersFromUnicodeAboveUsAsciiRange",
        "charactersFromUnicodeControlRanges",
        "charactersEscapedLowerCase",
        "charactersEscapedUpperCase",
    })
    void testWithAndWithoutTrailingSlash(ValidationStatus status, String location, String expected) throws Exception {
        testValidation(status, "/import/", location, expected);
        testValidation(status, "/import", location, expected);
    }

    /**
     * The business logic forbids query strings as there isn't a strong use case for them. Query strings in a URL need
     * to come after the path and need to begin with "?". Using the percent-encoding of "?" is allowed and is the way
     * <a href="https://www.rfc-editor.org/rfc/rfc3986#section-2.2">RFC3986#2.2</a> recommends encoding a "?" which
     * is not intended to be the beginning of a query string.
     *
     * @return test cases
     */
    private static Stream<Arguments> queryStringIsNotAllowed() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(ERR_QUERY, "file:/file?csv", null),
                        arg(ERR_QUERY, "file:/?csv", null),
                        arg(ERR_QUERY, "file:///?csv", null),
                        arg(ERR_ARG, "file:/?", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%3Fcsv", "file:/import/file%3Fcsv"),
                        arg(OK, "file:/%3Fcsv", "file:/import/%3Fcsv"),
                        arg(OK, "file:///%3F", "file:/import/%3F"),
                        arg(OK, "file:/%3F", "file:/import/%3F")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%253Fcsv", "file:/import/file%253Fcsv"),
                        arg(OK, "file:/%253Fcsv", "file:/import/%253Fcsv"),
                        arg(OK, "file:///%253F", "file:/import/%253F"),
                        arg(OK, "file:/%253F", "file:/import/%253F")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%25253Fcsv", "file:/import/file%25253Fcsv"),
                        arg(OK, "file:/file%2525253Fcsv", "file:/import/file%2525253Fcsv"),
                        arg(OK, "file:/file%252525253Fcsv", "file:/import/file%252525253Fcsv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/file%C0%BFcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"))); // "?"
    }

    /**
     * The business logic forbids query fragments as there isn't a strong use case for them. Fragments in a URL need to
     * come after the path and need to begin with "#".
     *
     * @return test cases
     */
    private static Stream<Arguments> fragmentIsNotAllowed() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(ERR_FRAGMENT, "file:/file#csv", null),
                        arg(ERR_FRAGMENT, "file:/#csv", null),
                        arg(ERR_FRAGMENT, "file:///#csv", null),
                        arg(ERR_ARG, "file:/#", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%23csv", "file:/import/file%23csv"),
                        arg(OK, "file:/%23csv", "file:/import/%23csv"),
                        arg(OK, "file:///%23csv", "file:/import/%23csv"),
                        arg(OK, "file:/%23", "file:/import/%23")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%2523csv", "file:/import/file%2523csv"),
                        arg(OK, "file:/%2523csv", "file:/import/%2523csv"),
                        arg(OK, "file:///%2523csv", "file:/import/%2523csv"),
                        arg(OK, "file:/%2523", "file:/import/%2523")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%252523csv", "file:/import/file%252523csv"),
                        arg(OK, "file:/file%25252523csv", "file:/import/file%25252523csv"),
                        arg(OK, "file:/file%2525252523csv", "file:/import/file%2525252523csv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/file%C0%A3csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"))); // "#"
    }

    /**
     * The business logic forbids authority as we don't want to allow users to retrieve a file over a network.
     * Authorities in a URL need to come after the scheme and need to start with ://. We've added a few extra test
     * cases which check that :/ and :/// which should be allowed. Authorities are forbidden because they would allow
     * a user to fetch a file from over the HTTP network, which is not something we want.
     *
     * @return test cases
     */
    private static Stream<Arguments> authorityIsNotAllowed() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv", "file:/import/file.csv"),
                        arg(ERR_AUTH, "file://file.csv", null),
                        arg(ERR_AUTH, "file://dir1/file.csv", null),
                        arg(ERR_AUTH, "file://:/file.csv", null),
                        arg(ERR_AUTH, "file://./file.csv", null),
                        arg(ERR_AUTH, "file://../file.csv", null),
                        arg(ERR_AUTH, "file://.file.csv", null),
                        arg(ERR_AUTH, "file://..file.csv", null),
                        arg(ERR_AUTH, "file://...file.csv", null),
                        arg(ERR_AUTH, "file://localhost/file.csv", null),
                        arg(ERR_AUTH, "file://localhost:80/file.csv", null),
                        arg(ERR_AUTH, "file://me.com/file.csv", null),
                        arg(ERR_AUTH, "file://w.me.com:80/file.csv", null),
                        arg(ERR_AUTH, "file://256.1.1.25/file.csv", null),
                        arg(ERR_AUTH, "file://256.1.1.25:80/file.csv", null),
                        arg(ERR_AUTH, "file://[::]/file.csv", null),
                        arg(ERR_AUTH, "file://[::]:80/file.csv", null),
                        arg(ERR_AUTH, "file://[1:1:1:1:1:1:1:1]/file.csv", null),
                        arg(ERR_AUTH, "file://[1:1:1:1:1:1:1:1]:80/file.csv", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2Ffile.csv", "file:/import/file.csv"),
                        arg(ERR_ARG, "file:%2F/file.csv", null),
                        arg(ERR_ARG, "file:%2F%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F%2F//file.csv", null),
                        arg(ERR_AUTH, "file://%2Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2F///////file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252F%252Ffile.csv", "file:/import/%252F%252Ffile.csv"),
                        arg(ERR_ARG, "file:%252F%252F%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%252F//%2EF////file.csv", null),
                        arg(ERR_AUTH, "file://%252Efile.csv", null),
                        arg(ERR_AUTH, "file://%252E%252Efile.csv", null),
                        arg(ERR_AUTH, "file://%253A/file.csv", null),
                        arg(ERR_AUTH, "file://me.com/file%252Ecsv", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252F%252Ffile.csv", "file:/import/%25252F%252Ffile.csv"),
                        arg(ERR_ARG, "file:%25252F%252F%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%25252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2525252Efile.csv", null),
                        arg(ERR_AUTH, "file://%2525252E%25252Efile.csv", null)),
                new OverlongPercentEncoded(
                        arg(OK, "file:/%C0%AFfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "/"
                        arg(ERR_ARG, "file:%C0%AF/file.csv", null), // "/"
                        arg(ERR_ARG, "file:%C0%AF%C0%AFfile.csv", null), // "/"
                        arg(ERR_AUTH, "file://%C0%AFfile.csv", null), // "/"
                        arg(OK, "file:/%C0%AEfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "."
                        arg(ERR_AUTH, "file://%C0%AEfile.csv", null), // "."
                        arg(OK, "file:/%C1%9Bfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "["
                        arg(ERR_AUTH, "file://%C1%9Bfile.csv", null), // "["
                        arg(OK, "file:/%C1%9Dfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "]"
                        arg(ERR_AUTH, "file://%C1%9Dfile.csv", null), // "]"
                        arg(OK, "file:/%C0%BAfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // ":"
                        arg(ERR_AUTH, "file://%C1%BAfile.csv", null))); // ":"
    }

    /**
     * The business logic allows paths which resemble authorities, because they are not authorities, and because it is a
     * legitimate use case for users to name their files as domain names or ip addresses. Paths in a URL need to come
     * after the scheme and the authority (if it exists).
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsThatResembleAuthorities() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/:file.csv", "file:/import/:file.csv"),
                        arg(OK, "file:/:80/file.csv", "file:/import/:80/file.csv"),
                        arg(OK, "file:/..:/file.csv", "file:/import/..:/file.csv"),
                        arg(OK, "file:/localhost/file.csv", "file:/import/localhost/file.csv"),
                        arg(OK, "file:/localhost:80/file.csv", "file:/import/localhost:80/file.csv"),
                        arg(OK, "file:/me.com/file.csv", "file:/import/me.com/file.csv"),
                        arg(OK, "file:/256.1.1.25/file.csv", "file:/import/256.1.1.25/file.csv"),
                        arg(OK, "file:/256.1.1.25:80/file.csv", "file:/import/256.1.1.25:80/file.csv"),
                        arg(OK, "file:/w.me.com:80/file.csv", "file:/import/w.me.com:80/file.csv"),
                        arg(OK, "file:///w.me.com:80/file.csv", "file:/import/w.me.com:80/file.csv"),
                        arg(OK, "file:///:file.csv", "file:/import/:file.csv"),
                        arg(OK, "file:/:/file.csv", "file:/import/:/file.csv"),
                        arg(OK, "file:/:/:file.csv", "file:/import/:/:file.csv"),
                        arg(OK, "file:///:/file.csv", "file:/import/:/file.csv"),
                        arg(OK, "file:/::file.csv", "file:/import/::file.csv"),
                        arg(OK, "file:///:::file.csv", "file:/import/:::file.csv"),
                        arg(ERR_ARG, "file::/file.csv", null),
                        arg(ERR_URL, "file://:file.csv", "file://:file.csv"),
                        arg(ERR_URL, "file://::file.csv", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2Efile.csv", "file:/import/.file.csv"),
                        arg(OK, "file:/%2E%2Efile.csv", "file:/import/..file.csv"),
                        arg(OK, "file:/%3Afile.csv", "file:/import/:file.csv"),
                        arg(OK, "file:/%3A/file.csv", "file:/import/:/file.csv"),
                        arg(OK, "file:///%3Afile.csv", "file:/import/:file.csv"),
                        arg(OK, "file:/:%3A:file.csv", "file:/import/:::file.csv"),
                        arg(OK, "file:/%3A:file.csv", "file:/import/::file.csv"),
                        arg(OK, "file:/%3A80%2Ffile.csv", "file:/import/:80/file.csv"),
                        arg(OK, "file:/localhost%3A80/file.csv", "file:/import/localhost:80/file.csv"),
                        arg(OK, "file:/me%2Ecom%3A80/file.csv", "file:/import/me.com:80/file.csv"),
                        arg(OK, "file:/%5B1%3A1:1:1:1:1%3A1:1%5D/csv", "file:/import/%5B1:1:1:1:1:1:1:1%5D/csv"),
                        arg(OK, "file:/%5B1:1:1:1:1:1:1:1%5D%3A80/c", "file:/import/%5B1:1:1:1:1:1:1:1%5D:80/c"),
                        arg(ERR_AUTH, "file://%3Afile.csv", null),
                        arg(ERR_AUTH, "file://%3A%3Afile.csv", null),
                        arg(ERR_URL, "file%3A%3A/file.csv", null),
                        arg(ERR_URL, "file%3A/file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/w%252Eme.com%253A80/csv", "file:/import/w%252Eme.com%253A80/csv"),
                        arg(OK, "file:///w.me%252Ecom:%252Fcsv", "file:/import/w.me%252Ecom:%252Fcsv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/w%25252Eme.com%25253A80/csv", "file:/import/w%25252Eme.com%25253A80/csv"),
                        arg(OK, "file:///w.me.com%2525253A80/csv", "file:/import/w.me.com%2525253A80/csv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/%C0%AFfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "/"
                        arg(OK, "file:///%C0%AFfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "/"
                        arg(OK, "file:/%C0%AEfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "."
                        arg(OK, "file:///%C0%AEfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "."
                        arg(OK, "file:/%C1%9Bfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "["
                        arg(OK, "file:///%C1%9Bfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "["
                        arg(OK, "file:/%C1%9Dfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "]"
                        arg(OK, "file:///%C1%9Dfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // "]"
                        arg(OK, "file:/%C0%BAfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"), // ":"
                        arg(OK, "file:///%C0%BAfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"))); // ":"
    }

    /**
     * The business logic allows paths which are empty, although there isn't a use case for this.
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsWhichAreEmpty() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/", "file:/import"),
                        arg(OK, "file:///", "file:/import"),
                        arg(ERR_URL, "file", null),
                        arg(ERR_URI, "file:", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2F%2F", "file:/import"),
                        arg(ERR_ARG, "file:%2F", null),
                        arg(ERR_URL, "file%3A", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252F%252F", "file:/import/%252F%252F"),
                        arg(ERR_ARG, "file:%252F", null),
                        arg(ERR_URL, "file%253A", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%2525252F%252F", "file:/import/%2525252F%252F"),
                        arg(ERR_ARG, "file:%25252F", null),
                        arg(ERR_URL, "file%25253A", null)),
                new OverlongPercentEncoded(
                        arg(ERR_ARG, "file:%C0%AF", null), // "/"
                        arg(ERR_URL, "file%C0%BA/", null))); // ":"
    }

    /**
     * The business logic allows paths which multiple leading slashes although there isn't a use case for this, and
     * these leading slashes get normalized away. Paths in a URL need to come after the scheme and the authority (if it
     * exists). Please note that it's not allowed to have a URL with two leading slashes because the
     * <a href="https://www.rfc-editor.org/rfc/rfc3986#section-2.2">RFC3986</a> considers this to be the beginning of
     * an authority, which we disallowed in our business logic (see pathsThatResembleAuthorities test cases).
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsWithLeadingSlashes() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///////file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/dir1/file.csv", "file:/import/dir1/file.csv"),
                        arg(OK, "file:///dir1/file.csv", "file:/import/dir1/file.csv"),
                        arg(OK, "file:////dir1/file.csv", "file:/import/dir1/file.csv"),
                        arg(OK, "file://///////dir1/file.csv", "file:/import/dir1/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:///%2F//%2F///file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F////%2F/////file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2F%2F%2F%2F%2F%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:///%2F%2F%2F%2F%2F%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(ERR_ARG, "file:%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F//file.csv", null),
                        arg(ERR_ARG, "file:%2F/%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F%2F/file.csv", null),
                        arg(ERR_ARG, "file:%2F%2F%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2Fdir1/file.csv", null),
                        arg(ERR_AUTH, "file://%2Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2F///////file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252Ffile%252Ecsv", "file:/import/%252Ffile%252Ecsv"),
                        arg(OK, "file:/%252Ffile%2520csv", "file:/import/%252Ffile%2520csv"),
                        arg(OK, "file:/%252Ffile.csv", "file:/import/%252Ffile.csv"),
                        arg(OK, "file:///%252Ffile.csv", "file:/import/%252Ffile.csv"),
                        arg(ERR_ARG, "file:%252F//file.csv", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252Ffile%2520csv", "file:/import/%25252Ffile%2520csv"),
                        arg(OK, "file:/%25252Ffile%25252520csv", "file:/import/%25252Ffile%25252520csv"),
                        arg(OK, "file:/%25252Ffile%20csv", "file:/import/%25252Ffile%20csv"),
                        arg(ERR_ARG, "file:%25252F//file.csv", null)),
                new OverlongPercentEncoded(
                        arg(OK, "file:/%C0%AFfile.csv", "file:/import/%EF%BF%BD%EF%BF%BDfile.csv"))); // "/"
    }

    /**
     * The business logic allows paths which multiple trailing slashes although there isn't a use case for this, and
     * these leading slashes get normalized away. Paths in a URL need to come after the scheme and the authority (if it
     * exists).
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsWithTrailingSlashes() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/file.csv/", "file:/import/file.csv"),
                        arg(OK, "file:/file.csv//", "file:/import/file.csv"),
                        arg(OK, "file:/file.csv///", "file:/import/file.csv"),
                        arg(OK, "file:/file.csv////", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv/", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv//", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv///", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv////", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv/", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv//", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv///", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv////", "file:/import/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/file.csv%2F", "file:/import/file.csv"),
                        arg(OK, "file:/file.csv%2F%2F%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv%2F", "file:/import/file.csv"),
                        arg(OK, "file:///file.csv%2F%2F%2F%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv%2F", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv%2F%2F%2F%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file:////file.csv%2F%2F%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2F%2Ffile.csv//", "file:/import/file.csv"),
                        arg(OK, "file:/%2F%2F%2Ffile.csv%2F%2F%2F%2F", "file:/import/file.csv"),
                        arg(OK, "file://///%2F%2F%2Ffile.csv//", "file:/import/file.csv"),
                        arg(OK, "file://///%2F%2F%2Ffile.csv%2F%2F%2F%2F", "file:/import/file.csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file.csv%252F", "file:/import/file.csv%252F"),
                        arg(OK, "file:/file.csv%252F%2F%2F%2F", "file:/import/file.csv%252F"),
                        arg(OK, "file:///file.csv%252F", "file:/import/file.csv%252F"),
                        arg(OK, "file:/%252F%252Ffile.csv%252F", "file:/import/%252F%252Ffile.csv%252F"),
                        arg(OK, "file:////file.csv%252F%252F", "file:/import/file.csv%252F%252F"),
                        arg(OK, "file:/%252F%252F%252Ffile.csv//", "file:/import/%252F%252F%252Ffile.csv"),
                        arg(OK, "file://///%252F%252F%252Ffile.csv//", "file:/import/%252F%252F%252Ffile.csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file.csv%25252F", "file:/import/file.csv%25252F"),
                        arg(OK, "file:/file.csv%2525252F%252F", "file:/import/file.csv%2525252F%252F"),
                        arg(OK, "file:/%25252F%25252Ffile.csv", "file:/import/%25252F%25252Ffile.csv"),
                        arg(OK, "file:////file.csv%25252F%25252F", "file:/import/file.csv%25252F%25252F"),
                        arg(OK, "file:/%25252Ffile.csv%25252F", "file:/import/%25252Ffile.csv%25252F"),
                        arg(OK, "file://///%25252F%25252Ffile.csv//", "file:/import/%25252F%25252Ffile.csv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/file.csv%C0%AF", "file:/import/file.csv%EF%BF%BD%EF%BF%BD"), // "/"
                        arg(OK, "file:////%C0%Afile.csv///", "file:/import/%EF%BF%BD%EF%BF%BDile.csv"))); // "/"
    }

    /**
     * The business logic allows path traversal although there isn't a strong use case for it. The business logic needs
     * to carefully validate that the result of the path traversal falls within the directory provided by the setting
     * GraphDatabaseSettings.load_csv_file_url_root.
     * <p>
     * Paths in a URL need to come after the scheme and the authority (if it
     * exists).
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsWithTraversal() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/.file.csv", "file:/import/.file.csv"),
                        arg(OK, "file:/..file.csv", "file:/import/..file.csv"),
                        arg(OK, "file:/...file.csv", "file:/import/...file.csv"),
                        arg(OK, "file:///.file.csv", "file:/import/.file.csv"),
                        arg(OK, "file:///..file.csv", "file:/import/..file.csv"),
                        arg(OK, "file:///...file.csv", "file:/import/...file.csv"),
                        arg(OK, "file:/./file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/~file.csv", "file:/import/~file.csv"),
                        arg(OK, "file:/~/file.csv", "file:/import/~/file.csv"),
                        arg(OK, "file:/../../~file.csv", "file:/import/~file.csv"),
                        arg(OK, "file:/../~/../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///./file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/dir1/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///dir1/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/..//dir1/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/../../dir1/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///import/../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/../import/file.csv", "file:/import/import/file.csv"),
                        arg(OK, "file:///../import/file.csv", "file:/import/import/file.csv"),
                        arg(OK, "file://///////../import/file.csv", "file:/import/import/file.csv"),
                        arg(OK, "file:/..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/..//..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///..//..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//..//..//../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//..//../..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////../..//../..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////../../../..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////../../../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//../..//../../file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..//..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file://////..//..//..//..//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/..///..///..///file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..///file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..///..///file.csv", "file:/import/file.csv"),
                        arg(OK, "file://///..///..///..///file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////.//.//file.csv", "file:/import/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2E%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///%2E%2E%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E%2E/%2E%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E%2E/%2E%2E/%2E%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E%2E%2F%2E%2E%2F%2E%2E%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E%2E%2Fimport/file.csv", "file:/import/import/file.csv"),
                        arg(OK, "file:/%2efile.csv", "file:/import/.file.csv"),
                        arg(OK, "file:/%2Efile.csv", "file:/import/.file.csv"),
                        arg(OK, "file:/%2Efile%2Ecsv", "file:/import/.file.csv"),
                        arg(OK, "file:///%2Efile.csv", "file:/import/.file.csv"),
                        arg(OK, "file:/%7Efile.csv", "file:/import/~file.csv"),
                        arg(OK, "file:/%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:///%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/../%7E/../file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E/%2E/%2E/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E/%2E/%2E/%2Efile.csv", "file:/import/.file.csv"),
                        arg(ERR_AUTH, "file://%2Efile.csv", null),
                        arg(ERR_AUTH, "file://%2E%2Efile.csv", null),
                        arg(OK, "file:/%2E%2E%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%2E./file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/..%2Ffile.csv", "file:/import/file.csv"),
                        // combinations of file:////..//..//file.csv
                        arg(OK, "file:/%2e%2e//%2e%2e//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..//..%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:////../%2F..%2F/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////%2e%2e//%2e%2e//file.csv", "file:/import/file.csv"),
                        arg(OK, "file:////..%2F%2F..%2F%2Ffile.csv", "file:/import/file.csv"),
                        arg(OK, "file:////%2E%2E%2F%2F%2E%2E%2F%2Ffile.csv", "file:/import/file.csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252E%252E/file.csv", "file:/import/%252E%252E/file.csv"),
                        arg(OK, "file:///%252E%252E%252Ffile.csv", "file:/import/%252E%252E%252Ffile.csv"),
                        arg(OK, "file:/%252E%252E/%252E/file.csv", "file:/import/%252E%252E/%252E/file.csv"),
                        arg(OK, "file:/%252E%252E%252Fimport/csv", "file:/import/%252E%252E%252Fimport/csv"),
                        arg(OK, "file:////..//..%252F%2Ffile.csv", "file:/import/..%252F/file.csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252E%25252E/file.csv", "file:/import/%25252E%25252E/file.csv"),
                        arg(OK, "file:///%2525252E%25252Ffile.csv", "file:/import/%2525252E%25252Ffile.csv"),
                        arg(OK, "file:////..//..%25252F%2Ffile.csv", "file:/import/..%25252F/file.csv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/%C0%AE./file.csv", "file:/import/%EF%BF%BD%EF%BF%BD./file.csv"), // "."
                        arg(OK, "file:/..%C0%AFfile.csv", "file:/import/..%EF%BF%BD%EF%BF%BDfile.csv"))); // "/"
    }

    /**
     * These are characters from the <a href="https://www.rfc-editor.org/rfc/rfc3986#section-2.2">RFC3986#2.2</a> which
     * are reserved.
     * <p>
     * Note that we've also included the control character "%"
     * <p>
     * Note that "[" and "]" are invalid in a path because the specifications mentions "the only place where square
     * bracket characters are allowed in the URI syntax (is in the authority)".
     * <p>
     * Note that "␣" is invalid because the specifications mentions "software that accepts user-typed URI
     * should attempt to recognize and strip both delimiters and embedded whitespace".
     * <p>
     * Note that characters ?#[]␣ still appear in their percent-encoded form in the SinglePercentEncoded test cases.
     * This is because the application decodes the original URLS, does computation on them, and then re-encodes them.
     * When they are re-encoded the application will percent-encode the values because they have special meaning in a
     * path or are otherwise forbidden. The specification mentions "if data for a URI component would conflict with a
     * reserved character's purpose as a delimiter, then the conflicting data must be percent-encoded before the URI is
     * formed"
     *
     * @return test cases
     */
    private static Stream<Arguments> charactersReservedFromUriProtocol() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(ERR_URI, "file:/file%csv", null),
                        arg(OK, "file:/file:csv", "file:/import/file:csv"),
                        arg(OK, "file:/file/csv", "file:/import/file/csv"),
                        arg(ERR_QUERY, "file:/file?csv", null),
                        arg(ERR_FRAGMENT, "file:/file#csv", null),
                        arg(OK, "file:/file@csv", "file:/import/file@csv"),
                        arg(OK, "file:/file!csv", "file:/import/file!csv"),
                        arg(OK, "file:/file$csv", "file:/import/file$csv"),
                        arg(OK, "file:/file&csv", "file:/import/file&csv"),
                        arg(OK, "file:/file'csv", "file:/import/file'csv"),
                        arg(OK, "file:/file(csv", "file:/import/file(csv"),
                        arg(OK, "file:/file)csv", "file:/import/file)csv"),
                        arg(OK, "file:/file*csv", "file:/import/file*csv"),
                        arg(OK, "file:/file+csv", "file:/import/file+csv"),
                        arg(OK, "file:/file,csv", "file:/import/file,csv"),
                        arg(OK, "file:/file;csv", "file:/import/file;csv"),
                        arg(OK, "file:/file=csv", "file:/import/file=csv"),
                        arg(ERR_URI, "file:/file[csv", null),
                        arg(ERR_URI, "file:/file]csv", null),
                        arg(ERR_URI, "file:/file csv", null)),
                new SinglePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%25csv", "file:/import/file%25csv"),
                        arg(OK, "file:/file%3Acsv", "file:/import/file:csv"),
                        arg(OK, "file:/file%2Fcsv", "file:/import/file/csv"),
                        arg(OK, "file:/file%3Fcsv", "file:/import/file%3Fcsv"),
                        arg(OK, "file:/file%23csv", "file:/import/file%23csv"),
                        arg(OK, "file:/file%40csv", "file:/import/file@csv"),
                        arg(OK, "file:/file%21csv", "file:/import/file!csv"),
                        arg(OK, "file:/file%24csv", "file:/import/file$csv"),
                        arg(OK, "file:/file%26csv", "file:/import/file&csv"),
                        arg(OK, "file:/file%27csv", "file:/import/file'csv"),
                        arg(OK, "file:/file%28csv", "file:/import/file(csv"),
                        arg(OK, "file:/file%29csv", "file:/import/file)csv"),
                        arg(OK, "file:/file%2Acsv", "file:/import/file*csv"),
                        arg(OK, "file:/file%2Bcsv", "file:/import/file+csv"),
                        arg(OK, "file:/file%2Ccsv", "file:/import/file,csv"),
                        arg(OK, "file:/file%3Bcsv", "file:/import/file;csv"),
                        arg(OK, "file:/file%3Dcsv", "file:/import/file=csv"),
                        arg(OK, "file:/file%5Bcsv", "file:/import/file%5Bcsv"),
                        arg(OK, "file:/file%5Dcsv", "file:/import/file%5Dcsv"),
                        arg(OK, "file:/file%20csv", "file:/import/file%20csv")),
                new DoublePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%2525csv", "file:/import/file%2525csv"),
                        arg(OK, "file:/file%253Acsv", "file:/import/file%253Acsv"),
                        arg(OK, "file:/file%252Fcsv", "file:/import/file%252Fcsv"),
                        arg(OK, "file:/file%253Fcsv", "file:/import/file%253Fcsv"),
                        arg(OK, "file:/file%2523csv", "file:/import/file%2523csv"),
                        arg(OK, "file:/file%2540csv", "file:/import/file%2540csv"),
                        arg(OK, "file:/file%2521csv", "file:/import/file%2521csv"),
                        arg(OK, "file:/file%2524csv", "file:/import/file%2524csv"),
                        arg(OK, "file:/file%2526csv", "file:/import/file%2526csv"),
                        arg(OK, "file:/file%2527csv", "file:/import/file%2527csv"),
                        arg(OK, "file:/file%2528csv", "file:/import/file%2528csv"),
                        arg(OK, "file:/file%2529csv", "file:/import/file%2529csv"),
                        arg(OK, "file:/file%252Acsv", "file:/import/file%252Acsv"),
                        arg(OK, "file:/file%252Bcsv", "file:/import/file%252Bcsv"),
                        arg(OK, "file:/file%252Ccsv", "file:/import/file%252Ccsv"),
                        arg(OK, "file:/file%253Bcsv", "file:/import/file%253Bcsv"),
                        arg(OK, "file:/file%253Dcsv", "file:/import/file%253Dcsv"),
                        arg(OK, "file:/file%255Bcsv", "file:/import/file%255Bcsv"),
                        arg(OK, "file:/file%255Dcsv", "file:/import/file%255Dcsv"),
                        arg(OK, "file:/file%2520csv", "file:/import/file%2520csv")),
                new TripleOrMorePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%252525csv", "file:/import/file%252525csv"),
                        arg(OK, "file:/file%25253Acsv", "file:/import/file%25253Acsv"),
                        arg(OK, "file:/file%25252Fcsv", "file:/import/file%25252Fcsv"),
                        arg(OK, "file:/file%25253Fcsv", "file:/import/file%25253Fcsv"),
                        arg(OK, "file:/file%252523csv", "file:/import/file%252523csv"),
                        arg(OK, "file:/file%252540csv", "file:/import/file%252540csv"),
                        arg(OK, "file:/file%252521csv", "file:/import/file%252521csv"),
                        arg(OK, "file:/file%252524csv", "file:/import/file%252524csv"),
                        arg(OK, "file:/file%252526csv", "file:/import/file%252526csv"),
                        arg(OK, "file:/file%252527csv", "file:/import/file%252527csv"),
                        arg(OK, "file:/file%252528csv", "file:/import/file%252528csv"),
                        arg(OK, "file:/file%252529csv", "file:/import/file%252529csv"),
                        arg(OK, "file:/file%25252Acsv", "file:/import/file%25252Acsv"),
                        arg(OK, "file:/file%25252Bcsv", "file:/import/file%25252Bcsv"),
                        arg(OK, "file:/file%25252Ccsv", "file:/import/file%25252Ccsv"),
                        arg(OK, "file:/file%25253Bcsv", "file:/import/file%25253Bcsv"),
                        arg(OK, "file:/file%25253Dcsv", "file:/import/file%25253Dcsv"),
                        arg(OK, "file:/file%25255Bcsv", "file:/import/file%25255Bcsv"),
                        arg(OK, "file:/file%25255Dcsv", "file:/import/file%25255Dcsv"),
                        arg(OK, "file:/file%252520csv", "file:/import/file%252520csv")),
                new OverlongPercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%C0%A5csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%BAcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%AFcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%BFcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A3csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%C0csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A1csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A4csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A6csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A7csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A8csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A9csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%AAcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%ABcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%ACcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%BBcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%ADcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C1%9Bcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C1%9Dcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%A0csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv")));
    }

    /**
     * These are special characters from the <a href="https://url.spec.whatwg.org/#application/x-w-form-urlencoded">
     * application/x-w-form-urlencoded</a> standard is what the more modern
     * <a href="https://www.rfc-editor.org/rfc/rfc3986">RFC3986</a> is derived from.
     * <p>
     * The only special character is SPACE which is encoded as PLUS. Note that whitespaces are forbidden by both
     * standards.
     *
     * @return test cases
     */
    private static Stream<Arguments> charactersReservedFromApplicationForms() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(ERR_URI, "file:/file csv", null), arg(OK, "file:/file+csv", "file:/import/file+csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%20csv", "file:/import/file%20csv"),
                        arg(OK, "file:/file%2Bcsv", "file:/import/file+csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%2520csv", "file:/import/file%2520csv"),
                        arg(OK, "file:/file%252Bcsv", "file:/import/file%252Bcsv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%252520csv", "file:/import/file%252520csv"),
                        arg(OK, "file:/file%25252Bcsv", "file:/import/file%25252Bcsv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/file%C0%A0csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"), // " "
                        arg(OK, "file:/file%C0%ABcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"))); // "+"
    }

    /**
     * These are unicode characters within the US_ASCII range. They represent code points \u0020 to \u007E. This range
     * of characters is because UTF-8 tries to be compatible with US_ASCII, where a string which is encoded in US_ASCII
     * should be a valid UTF-8 file. The problem is that US_ASCII is a single-byte encoding scheme while UTF-8 is
     * multibyte. In order to achieve its compatibility goals, UTF-8 represents the range of characters from US_ASCII
     * as single-byte, making this range of characters special.
     *
     * @return test cases
     */
    private static Stream<Arguments> charactersFromUnicodeInUsAsciiRange() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/file.csv", "file:/import/file.csv"),
                        arg(OK, "file:/fil\u0065.csv", "file:/import/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/fil%65.csv", "file:/import/file.csv"),
                        arg(OK, "file:/%66ile.csv", "file:/import/file.csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/fil%2565.csv", "file:/import/fil%2565.csv"),
                        arg(OK, "file:/%2566ile.csv", "file:/import/%2566ile.csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/fil%252565.csv", "file:/import/fil%252565.csv"),
                        arg(OK, "file:/%252566ile.csv", "file:/import/%252566ile.csv")),
                new OverlongPercentEncoded(
                        arg(OK, "file:/fil%C1%A5.csv", "file:/import/fil%EF%BF%BD%EF%BF%BD.csv"), // "e"
                        arg(OK, "file:/fil%C1%A6.csv", "file:/import/fil%EF%BF%BD%EF%BF%BD.csv"))); // "f"
    }

    /**
     *  These are unicode characters within the US_ASCII range, which should always be multibyte.
     *
     * @return test cases
     */
    private static Stream<Arguments> charactersFromUnicodeAboveUsAsciiRange() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/file¥csv", "file:/import/file%C2%A5csv"),
                        arg(OK, "file:/file£csv", "file:/import/file%C2%A3csv"),
                        arg(OK, "file:/file\u00A3csv", "file:/import/file%C2%A3csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%C2%A5csv", "file:/import/file%C2%A5csv"),
                        arg(OK, "file:/file%C2%A3csv", "file:/import/file%C2%A3csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%25C2%25A5csv", "file:/import/file%25C2%25A5csv"),
                        arg(OK, "file:/file%25C2%25A3csv", "file:/import/file%25C2%25A3csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%2525C2%25A5csv", "file:/import/file%2525C2%25A5csv"),
                        arg(OK, "file:/file%2525C2%25A3csv", "file:/import/file%2525C2%25A3csv")),
                new OverlongPercentEncoded()); // NA
    }

    /**
     * These are unicode characters within the C0 and C1 ranges of Unicode (\u0000 to \u0032) which are so-called
     * control characters because they can have an effect on the overall sequence (backspace, null character, cancel,
     * etc).
     * <p>
     * Note that Javac will preprocess UTF-8 encoded characters before the compiler is run. Characters such as u000d
     * (CR) and u000a (LF) therefor cannot be easily tested. We use the replacements \r and \n instead.
     * <p>
     * Note that Java has several other special
     * <a href="https://docs.oracle.com/javase/tutorial/java/data/characters.html">escape character sequences</a>.
     * We are not testing for those specifically because they all translate to unicode values which we cover in this
     * test and in others.
     *
     * @return test cases
     */
    private static Stream<Arguments> charactersFromUnicodeControlRanges() {
        return buildTestCases(
                new NonPercentEncoded(
                        // C0 range
                        arg(ERR_URI, "file:/file\u0000csv", null),
                        arg(ERR_URI, "file:/file\u0001csv", null),
                        arg(ERR_URI, "file:/file\u0002csv", null),
                        arg(ERR_URI, "file:/file\u0003csv", null),
                        arg(ERR_URI, "file:/file\u0004csv", null),
                        arg(ERR_URI, "file:/file\u0005csv", null),
                        arg(ERR_URI, "file:/file\u0006csv", null),
                        arg(ERR_URI, "file:/file\u0007csv", null),
                        arg(ERR_URI, "file:/file\u0008csv", null),
                        arg(ERR_URI, "file:/file\u0009csv", null),
                        arg(ERR_URI, "file:/file\ncsv", null),
                        arg(ERR_URI, "file:/file\u000bcsv", null),
                        arg(ERR_URI, "file:/file\u000ccsv", null),
                        arg(ERR_URI, "file:/file\rcsv", null),
                        arg(ERR_URI, "file:/file\u000ecsv", null),
                        arg(ERR_URI, "file:/file\u0010csv", null),
                        arg(ERR_URI, "file:/file\u0011csv", null),
                        arg(ERR_URI, "file:/file\u0012csv", null),
                        arg(ERR_URI, "file:/file\u0013csv", null),
                        arg(ERR_URI, "file:/file\u0014csv", null),
                        arg(ERR_URI, "file:/file\u0015csv", null),
                        arg(ERR_URI, "file:/file\u0016csv", null),
                        arg(ERR_URI, "file:/file\u0017csv", null),
                        arg(ERR_URI, "file:/file\u0018csv", null),
                        arg(ERR_URI, "file:/file\u0019csv", null),
                        arg(ERR_URI, "file:/file\u001acsv", null),
                        arg(ERR_URI, "file:/file\u001bcsv", null),
                        arg(ERR_URI, "file:/file\u001ccsv", null),
                        arg(ERR_URI, "file:/file\u001dcsv", null),
                        arg(ERR_URI, "file:/file\u001ecsv", null),
                        arg(ERR_URI, "file:/file\u001fsv", null),

                        // C1 range
                        arg(ERR_URI, "file:/file\u0080sv", null),
                        arg(ERR_URI, "file:/file\u0081sv", null),
                        arg(ERR_URI, "file:/file\u0082sv", null),
                        arg(ERR_URI, "file:/file\u0083sv", null),
                        arg(ERR_URI, "file:/file\u0084sv", null),
                        arg(ERR_URI, "file:/file\u0085sv", null),
                        arg(ERR_URI, "file:/file\u0086sv", null),
                        arg(ERR_URI, "file:/file\u0087sv", null),
                        arg(ERR_URI, "file:/file\u0088sv", null),
                        arg(ERR_URI, "file:/file\u0089sv", null),
                        arg(ERR_URI, "file:/file\u008Asv", null),
                        arg(ERR_URI, "file:/file\u008Bsv", null),
                        arg(ERR_URI, "file:/file\u008Csv", null),
                        arg(ERR_URI, "file:/file\u008Dsv", null),
                        arg(ERR_URI, "file:/file\u008Esv", null),
                        arg(ERR_URI, "file:/file\u008Fsv", null),
                        arg(ERR_URI, "file:/file\u0090sv", null),
                        arg(ERR_URI, "file:/file\u0091sv", null),
                        arg(ERR_URI, "file:/file\u0092sv", null),
                        arg(ERR_URI, "file:/file\u0093sv", null),
                        arg(ERR_URI, "file:/file\u0094sv", null),
                        arg(ERR_URI, "file:/file\u0095sv", null),
                        arg(ERR_URI, "file:/file\u0096sv", null),
                        arg(ERR_URI, "file:/file\u0097sv", null),
                        arg(ERR_URI, "file:/file\u0098sv", null),
                        arg(ERR_URI, "file:/file\u0099sv", null),
                        arg(ERR_URI, "file:/file\u009Asv", null),
                        arg(ERR_URI, "file:/file\u009Bsv", null),
                        arg(ERR_URI, "file:/file\u009Csv", null),
                        arg(ERR_URI, "file:/file\u009Dsv", null),
                        arg(ERR_URI, "file:/file\u009Esv", null),
                        arg(ERR_URI, "file:/file\u009Fsv", null)),
                new SinglePercentEncoded(
                        // C0 in the same order as above
                        arg(ERR_PATH, "file:/file%00csv", null),
                        arg(OK, "file:/file%01csv", "file:/import/file%01csv"),
                        arg(OK, "file:/file%02csv", "file:/import/file%02csv"),
                        arg(OK, "file:/file%03csv", "file:/import/file%03csv"),
                        arg(OK, "file:/file%04csv", "file:/import/file%04csv"),
                        arg(OK, "file:/file%05csv", "file:/import/file%05csv"),
                        arg(OK, "file:/file%06csv", "file:/import/file%06csv"),
                        arg(OK, "file:/file%07csv", "file:/import/file%07csv"),
                        arg(OK, "file:/file%08csv", "file:/import/file%08csv"),
                        arg(OK, "file:/file%09csv", "file:/import/file%09csv"),
                        arg(OK, "file:/file%0Acsv", "file:/import/file%0Acsv"),
                        arg(OK, "file:/file%0Bcsv", "file:/import/file%0Bcsv"),
                        arg(OK, "file:/file%0Ccsv", "file:/import/file%0Ccsv"),
                        arg(OK, "file:/file%0Dcsv", "file:/import/file%0Dcsv"),
                        arg(OK, "file:/file%0Ecsv", "file:/import/file%0Ecsv"),
                        arg(OK, "file:/file%0Fcsv", "file:/import/file%0Fcsv"),
                        arg(OK, "file:/file%10csv", "file:/import/file%10csv"),
                        arg(OK, "file:/file%11csv", "file:/import/file%11csv"),
                        arg(OK, "file:/file%12csv", "file:/import/file%12csv"),
                        arg(OK, "file:/file%13csv", "file:/import/file%13csv"),
                        arg(OK, "file:/file%14csv", "file:/import/file%14csv"),
                        arg(OK, "file:/file%15csv", "file:/import/file%15csv"),
                        arg(OK, "file:/file%16csv", "file:/import/file%16csv"),
                        arg(OK, "file:/file%17csv", "file:/import/file%17csv"),
                        arg(OK, "file:/file%18csv", "file:/import/file%18csv"),
                        arg(OK, "file:/file%19csv", "file:/import/file%19csv"),
                        arg(OK, "file:/file%1Acsv", "file:/import/file%1Acsv"),
                        arg(OK, "file:/file%1Bcsv", "file:/import/file%1Bcsv"),
                        arg(OK, "file:/file%1Ccsv", "file:/import/file%1Ccsv"),
                        arg(OK, "file:/file%1Dcsv", "file:/import/file%1Dcsv"),
                        arg(OK, "file:/file%1Ecsv", "file:/import/file%1Ecsv"),
                        arg(OK, "file:/file%1Fcsv", "file:/import/file%1Fcsv"),

                        // C1 in the same order as above
                        arg(OK, "file:/file%C2%80csv", "file:/import/file%C2%80csv"),
                        arg(OK, "file:/file%C2%81csv", "file:/import/file%C2%81csv"),
                        arg(OK, "file:/file%C2%82csv", "file:/import/file%C2%82csv"),
                        arg(OK, "file:/file%C2%83csv", "file:/import/file%C2%83csv"),
                        arg(OK, "file:/file%C2%84csv", "file:/import/file%C2%84csv"),
                        arg(OK, "file:/file%C2%85csv", "file:/import/file%C2%85csv"),
                        arg(OK, "file:/file%C2%86csv", "file:/import/file%C2%86csv"),
                        arg(OK, "file:/file%C2%87csv", "file:/import/file%C2%87csv"),
                        arg(OK, "file:/file%C2%88csv", "file:/import/file%C2%88csv"),
                        arg(OK, "file:/file%C2%89csv", "file:/import/file%C2%89csv"),
                        arg(OK, "file:/file%C2%8Acsv", "file:/import/file%C2%8Acsv"),
                        arg(OK, "file:/file%C2%8Bcsv", "file:/import/file%C2%8Bcsv"),
                        arg(OK, "file:/file%C2%8Ccsv", "file:/import/file%C2%8Ccsv"),
                        arg(OK, "file:/file%C2%8Dcsv", "file:/import/file%C2%8Dcsv"),
                        arg(OK, "file:/file%C2%8Ecsv", "file:/import/file%C2%8Ecsv"),
                        arg(OK, "file:/file%C2%8Fcsv", "file:/import/file%C2%8Fcsv"),
                        arg(OK, "file:/file%C2%90csv", "file:/import/file%C2%90csv"),
                        arg(OK, "file:/file%C2%91csv", "file:/import/file%C2%91csv"),
                        arg(OK, "file:/file%C2%92csv", "file:/import/file%C2%92csv"),
                        arg(OK, "file:/file%C2%93csv", "file:/import/file%C2%93csv"),
                        arg(OK, "file:/file%C2%94csv", "file:/import/file%C2%94csv"),
                        arg(OK, "file:/file%C2%95csv", "file:/import/file%C2%95csv"),
                        arg(OK, "file:/file%C2%96csv", "file:/import/file%C2%96csv"),
                        arg(OK, "file:/file%C2%97csv", "file:/import/file%C2%97csv"),
                        arg(OK, "file:/file%C2%98csv", "file:/import/file%C2%98csv"),
                        arg(OK, "file:/file%C2%99csv", "file:/import/file%C2%99csv"),
                        arg(OK, "file:/file%C2%9Acsv", "file:/import/file%C2%9Acsv"),
                        arg(OK, "file:/file%C2%9Bcsv", "file:/import/file%C2%9Bcsv"),
                        arg(OK, "file:/file%C2%9Ccsv", "file:/import/file%C2%9Ccsv"),
                        arg(OK, "file:/file%C2%9Dcsv", "file:/import/file%C2%9Dcsv"),
                        arg(OK, "file:/file%C2%9Ecsv", "file:/import/file%C2%9Ecsv"),
                        arg(OK, "file:/file%C2%9Fcsv", "file:/import/file%C2%9Fcsv")),
                new DoublePercentEncoded(
                        // C0 in the same order as above
                        arg(OK, "file:/file%2500csv", "file:/import/file%2500csv"),
                        arg(OK, "file:/file%2501csv", "file:/import/file%2501csv"),
                        arg(OK, "file:/file%2502csv", "file:/import/file%2502csv"),
                        arg(OK, "file:/file%2503csv", "file:/import/file%2503csv"),
                        arg(OK, "file:/file%2504csv", "file:/import/file%2504csv"),
                        arg(OK, "file:/file%2505csv", "file:/import/file%2505csv"),
                        arg(OK, "file:/file%2506csv", "file:/import/file%2506csv"),
                        arg(OK, "file:/file%2507csv", "file:/import/file%2507csv"),
                        arg(OK, "file:/file%2508csv", "file:/import/file%2508csv"),
                        arg(OK, "file:/file%2509csv", "file:/import/file%2509csv"),
                        arg(OK, "file:/file%250Acsv", "file:/import/file%250Acsv"),
                        arg(OK, "file:/file%250Bcsv", "file:/import/file%250Bcsv"),
                        arg(OK, "file:/file%250Ccsv", "file:/import/file%250Ccsv"),
                        arg(OK, "file:/file%250Dcsv", "file:/import/file%250Dcsv"),
                        arg(OK, "file:/file%250Ecsv", "file:/import/file%250Ecsv"),
                        arg(OK, "file:/file%250Fcsv", "file:/import/file%250Fcsv"),
                        arg(OK, "file:/file%2510csv", "file:/import/file%2510csv"),
                        arg(OK, "file:/file%2511csv", "file:/import/file%2511csv"),
                        arg(OK, "file:/file%2512csv", "file:/import/file%2512csv"),
                        arg(OK, "file:/file%2513csv", "file:/import/file%2513csv"),
                        arg(OK, "file:/file%2514csv", "file:/import/file%2514csv"),
                        arg(OK, "file:/file%2515csv", "file:/import/file%2515csv"),
                        arg(OK, "file:/file%2516csv", "file:/import/file%2516csv"),
                        arg(OK, "file:/file%2517csv", "file:/import/file%2517csv"),
                        arg(OK, "file:/file%2518csv", "file:/import/file%2518csv"),
                        arg(OK, "file:/file%2519csv", "file:/import/file%2519csv"),
                        arg(OK, "file:/file%251Acsv", "file:/import/file%251Acsv"),
                        arg(OK, "file:/file%251Bcsv", "file:/import/file%251Bcsv"),
                        arg(OK, "file:/file%251Ccsv", "file:/import/file%251Ccsv"),
                        arg(OK, "file:/file%251Dcsv", "file:/import/file%251Dcsv"),
                        arg(OK, "file:/file%251Ecsv", "file:/import/file%251Ecsv"),
                        arg(OK, "file:/file%251Fcsv", "file:/import/file%251Fcsv"),

                        // C1 in the same order as above
                        arg(OK, "file:/file%25C2%2580csv", "file:/import/file%25C2%2580csv"),
                        arg(OK, "file:/file%25C2%2581csv", "file:/import/file%25C2%2581csv"),
                        arg(OK, "file:/file%25C2%2582csv", "file:/import/file%25C2%2582csv"),
                        arg(OK, "file:/file%25C2%2583csv", "file:/import/file%25C2%2583csv"),
                        arg(OK, "file:/file%25C2%2584csv", "file:/import/file%25C2%2584csv"),
                        arg(OK, "file:/file%25C2%2585csv", "file:/import/file%25C2%2585csv"),
                        arg(OK, "file:/file%25C2%2586csv", "file:/import/file%25C2%2586csv"),
                        arg(OK, "file:/file%25C2%2587csv", "file:/import/file%25C2%2587csv"),
                        arg(OK, "file:/file%25C2%2588csv", "file:/import/file%25C2%2588csv"),
                        arg(OK, "file:/file%25C2%2589csv", "file:/import/file%25C2%2589csv"),
                        arg(OK, "file:/file%25C2%258Acsv", "file:/import/file%25C2%258Acsv"),
                        arg(OK, "file:/file%25C2%258Bcsv", "file:/import/file%25C2%258Bcsv"),
                        arg(OK, "file:/file%25C2%258Ccsv", "file:/import/file%25C2%258Ccsv"),
                        arg(OK, "file:/file%25C2%258Dcsv", "file:/import/file%25C2%258Dcsv"),
                        arg(OK, "file:/file%25C2%258Ecsv", "file:/import/file%25C2%258Ecsv"),
                        arg(OK, "file:/file%25C2%258Fcsv", "file:/import/file%25C2%258Fcsv"),
                        arg(OK, "file:/file%25C2%2590csv", "file:/import/file%25C2%2590csv"),
                        arg(OK, "file:/file%25C2%2591csv", "file:/import/file%25C2%2591csv"),
                        arg(OK, "file:/file%25C2%2592csv", "file:/import/file%25C2%2592csv"),
                        arg(OK, "file:/file%25C2%2593csv", "file:/import/file%25C2%2593csv"),
                        arg(OK, "file:/file%25C2%2594csv", "file:/import/file%25C2%2594csv"),
                        arg(OK, "file:/file%25C2%2595csv", "file:/import/file%25C2%2595csv"),
                        arg(OK, "file:/file%25C2%2596csv", "file:/import/file%25C2%2596csv"),
                        arg(OK, "file:/file%25C2%2597csv", "file:/import/file%25C2%2597csv"),
                        arg(OK, "file:/file%25C2%2598csv", "file:/import/file%25C2%2598csv"),
                        arg(OK, "file:/file%25C2%2599csv", "file:/import/file%25C2%2599csv"),
                        arg(OK, "file:/file%25C2%259Acsv", "file:/import/file%25C2%259Acsv"),
                        arg(OK, "file:/file%25C2%259Bcsv", "file:/import/file%25C2%259Bcsv"),
                        arg(OK, "file:/file%25C2%259Ccsv", "file:/import/file%25C2%259Ccsv"),
                        arg(OK, "file:/file%25C2%259Dcsv", "file:/import/file%25C2%259Dcsv"),
                        arg(OK, "file:/file%25C2%259Ecsv", "file:/import/file%25C2%259Ecsv"),
                        arg(OK, "file:/file%25C2%259Fcsv", "file:/import/file%25C2%259Fcsv")),
                new TripleOrMorePercentEncoded(
                        // C0 in the same order as above
                        arg(OK, "file:/file%252500csv", "file:/import/file%252500csv"),
                        arg(OK, "file:/file%252501csv", "file:/import/file%252501csv"),
                        arg(OK, "file:/file%252502csv", "file:/import/file%252502csv"),
                        arg(OK, "file:/file%252503csv", "file:/import/file%252503csv"),
                        arg(OK, "file:/file%252504csv", "file:/import/file%252504csv"),
                        arg(OK, "file:/file%252505csv", "file:/import/file%252505csv"),
                        arg(OK, "file:/file%252506csv", "file:/import/file%252506csv"),
                        arg(OK, "file:/file%252507csv", "file:/import/file%252507csv"),
                        arg(OK, "file:/file%252508csv", "file:/import/file%252508csv"),
                        arg(OK, "file:/file%252509csv", "file:/import/file%252509csv"),
                        arg(OK, "file:/file%25250Acsv", "file:/import/file%25250Acsv"),
                        arg(OK, "file:/file%25250Bcsv", "file:/import/file%25250Bcsv"),
                        arg(OK, "file:/file%25250Ccsv", "file:/import/file%25250Ccsv"),
                        arg(OK, "file:/file%25250Dcsv", "file:/import/file%25250Dcsv"),
                        arg(OK, "file:/file%25250Ecsv", "file:/import/file%25250Ecsv"),
                        arg(OK, "file:/file%25250Fcsv", "file:/import/file%25250Fcsv"),
                        arg(OK, "file:/file%252510csv", "file:/import/file%252510csv"),
                        arg(OK, "file:/file%252511csv", "file:/import/file%252511csv"),
                        arg(OK, "file:/file%252512csv", "file:/import/file%252512csv"),
                        arg(OK, "file:/file%252513csv", "file:/import/file%252513csv"),
                        arg(OK, "file:/file%252514csv", "file:/import/file%252514csv"),
                        arg(OK, "file:/file%252515csv", "file:/import/file%252515csv"),
                        arg(OK, "file:/file%252516csv", "file:/import/file%252516csv"),
                        arg(OK, "file:/file%252517csv", "file:/import/file%252517csv"),
                        arg(OK, "file:/file%252518csv", "file:/import/file%252518csv"),
                        arg(OK, "file:/file%252519csv", "file:/import/file%252519csv"),
                        arg(OK, "file:/file%25251Acsv", "file:/import/file%25251Acsv"),
                        arg(OK, "file:/file%25251Bcsv", "file:/import/file%25251Bcsv"),
                        arg(OK, "file:/file%25251Ccsv", "file:/import/file%25251Ccsv"),
                        arg(OK, "file:/file%25251Dcsv", "file:/import/file%25251Dcsv"),
                        arg(OK, "file:/file%25251Ecsv", "file:/import/file%25251Ecsv"),
                        arg(OK, "file:/file%25251Fcsv", "file:/import/file%25251Fcsv"),

                        // C1 in the same order as above
                        arg(OK, "file:/file%2525C2%2580csv", "file:/import/file%2525C2%2580csv"),
                        arg(OK, "file:/file%2525C2%2581csv", "file:/import/file%2525C2%2581csv"),
                        arg(OK, "file:/file%2525C2%2582csv", "file:/import/file%2525C2%2582csv"),
                        arg(OK, "file:/file%2525C2%2583csv", "file:/import/file%2525C2%2583csv"),
                        arg(OK, "file:/file%2525C2%2584csv", "file:/import/file%2525C2%2584csv"),
                        arg(OK, "file:/file%2525C2%2585csv", "file:/import/file%2525C2%2585csv"),
                        arg(OK, "file:/file%2525C2%2586csv", "file:/import/file%2525C2%2586csv"),
                        arg(OK, "file:/file%2525C2%2587csv", "file:/import/file%2525C2%2587csv"),
                        arg(OK, "file:/file%2525C2%2588csv", "file:/import/file%2525C2%2588csv"),
                        arg(OK, "file:/file%2525C2%2589csv", "file:/import/file%2525C2%2589csv"),
                        arg(OK, "file:/file%2525C2%258Acsv", "file:/import/file%2525C2%258Acsv"),
                        arg(OK, "file:/file%2525C2%258Bcsv", "file:/import/file%2525C2%258Bcsv"),
                        arg(OK, "file:/file%2525C2%258Ccsv", "file:/import/file%2525C2%258Ccsv"),
                        arg(OK, "file:/file%2525C2%258Dcsv", "file:/import/file%2525C2%258Dcsv"),
                        arg(OK, "file:/file%2525C2%258Ecsv", "file:/import/file%2525C2%258Ecsv"),
                        arg(OK, "file:/file%2525C2%258Fcsv", "file:/import/file%2525C2%258Fcsv"),
                        arg(OK, "file:/file%2525C2%2590csv", "file:/import/file%2525C2%2590csv"),
                        arg(OK, "file:/file%2525C2%2591csv", "file:/import/file%2525C2%2591csv"),
                        arg(OK, "file:/file%2525C2%2592csv", "file:/import/file%2525C2%2592csv"),
                        arg(OK, "file:/file%2525C2%2593csv", "file:/import/file%2525C2%2593csv"),
                        arg(OK, "file:/file%2525C2%2594csv", "file:/import/file%2525C2%2594csv"),
                        arg(OK, "file:/file%2525C2%2595csv", "file:/import/file%2525C2%2595csv"),
                        arg(OK, "file:/file%2525C2%2596csv", "file:/import/file%2525C2%2596csv"),
                        arg(OK, "file:/file%2525C2%2597csv", "file:/import/file%2525C2%2597csv"),
                        arg(OK, "file:/file%2525C2%2598csv", "file:/import/file%2525C2%2598csv"),
                        arg(OK, "file:/file%2525C2%2599csv", "file:/import/file%2525C2%2599csv"),
                        arg(OK, "file:/file%2525C2%259Acsv", "file:/import/file%2525C2%259Acsv"),
                        arg(OK, "file:/file%2525C2%259Bcsv", "file:/import/file%2525C2%259Bcsv"),
                        arg(OK, "file:/file%2525C2%259Ccsv", "file:/import/file%2525C2%259Ccsv"),
                        arg(OK, "file:/file%2525C2%259Dcsv", "file:/import/file%2525C2%259Dcsv"),
                        arg(OK, "file:/file%2525C2%259Ecsv", "file:/import/file%2525C2%259Ecsv"),
                        arg(OK, "file:/file%2525C2%259Fcsv", "file:/import/file%2525C2%259Fcsv")),
                new OverlongPercentEncoded(
                        // C0 in the same order as above
                        arg(OK, "file:/file%C0%80csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%81csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%82csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%83csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%84csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%85csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%86csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%87csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%88csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%89csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Acsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Bcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Ccsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Dcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Ecsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%8Fcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%90csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%91csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%92csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%93csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%94csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%95csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%96csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%97csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%98csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%99csv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Acsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Bcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Ccsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Dcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Ecsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv"),
                        arg(OK, "file:/file%C0%9Fcsv", "file:/import/file%EF%BF%BD%EF%BF%BDcsv")

                        // C1 range (NA because already multibyte)
                        ));
    }

    private static Stream<Arguments> charactersEscapedLowerCase() {
        return Stream.of(
                arg(OK, "file:/file%2ecsv", "file:/import/file.csv"),
                arg(OK, "file:/file%2fcsv", "file:/import/file/csv"),
                arg(OK, "file:/file%c2%a3csv", "file:/import/file%C2%A3csv"));
    }

    private static Stream<Arguments> charactersEscapedUpperCase() {
        return Stream.of(
                arg(OK, "file:/file%2Ecsv", "file:/import/file.csv"),
                arg(OK, "file:/file%2Fcsv", "file:/import/file/csv"),
                arg(OK, "file:/file%C2%A3csv", "file:/import/file%C2%A3csv"));
    }

    protected enum ValidationStatus {
        OK, // Valid URL
        ERR_AUTH, // Invalid URL because it contains an authority
        ERR_QUERY, // Invalid URL because it contains a query string
        ERR_FRAGMENT, // Invalid URL because it contains a query fragment
        ERR_URL, // Syntactic error because URL can't be created from String
        ERR_URI, // Syntactic error because URL can't be converted to URI
        ERR_PATH, // Syntactic error because URL can't be converted to Path
        ERR_ARG, // Syntactic error because URL can't be created from String
    }

    private void testValidation(ValidationStatus status, String root, String url, String expected) throws Exception {
        if (status.equals(OK)) {
            URL accessURL = validate(root, url);
            assertEquals(expected, accessURL.toString());
        } else if (status.equals(ERR_AUTH)) {
            final var err = assertThrows(URLAccessValidationError.class, () -> validate(root, url));
            MatcherAssert.assertThat(err.getMessage(), containsString("URL may not contain an authority section"));
        } else if (status.equals(ERR_QUERY)) {
            final var err = assertThrows(URLAccessValidationError.class, () -> validate(root, url));
            MatcherAssert.assertThat(err.getMessage(), containsString("file URL may not contain a query component"));
        } else if (status.equals(ERR_FRAGMENT)) {
            final var err = assertThrows(IllegalArgumentException.class, () -> validate(root, url));
            MatcherAssert.assertThat(err.getMessage(), containsString("URI has a fragment component"));
        } else if (status.equals(ERR_ARG)) {
            assertThrows(IllegalArgumentException.class, () -> validate(root, url));
        } else if (status.equals(ERR_PATH)) {
            assertThrows(InvalidPathException.class, () -> validate(root, url));
        } else if (status.equals(ERR_URL)) {
            assertThrows(MalformedURLException.class, () -> validate(root, url));
        } else if (status.equals(ERR_URI)) {
            final var err = assertThrows(Exception.class, () -> validate(root, url));
            if (err instanceof RuntimeException) {
                assertEquals(err.getCause().getClass(), URISyntaxException.class);
            } else if (err instanceof URISyntaxException) {
                assertThrows(URISyntaxException.class, () -> validate(root, url));
            } else {
                throw new Exception("Unpexcted test result");
            }
        } else {
            throw new Exception("Unexpected test result");
        }
    }

    private URL validate(String root, String url) throws MalformedURLException, URLAccessValidationError {
        final Config config = Config.defaults(GraphDatabaseSettings.load_csv_file_url_root, Path.of(root));
        return URLAccessRules.fileAccess().validate(config, new URL(url));
    }

    /**
     * This record should contain tests cases which are not percent-encoded.
     *
     * @param entries Represents individual test cases
     */
    private record NonPercentEncoded(Arguments... entries) {}

    /**
     * This record should contain tests cases which are percent-encoded. Percent-encoding is how URL encode unicode
     * values. For example, '%38' represents a percent-encoded '&'. Please read
     * <a href="https://en.wikipedia.org/wiki/Percent-encoding">Wikipedia: Percent-Encoding</a> or
     * <a href="https://www.rfc-editor.org/rfc/rfc3986#section-2.1">RFC3986#percent-encoding</a> for more information.
     * Please use <a href="https://www.url-encode-decode.com/">url-encode-decode.com</a> to encode and decode characters
     * to their corresponding percent-encoded representation.
     *
     * @param entries Represents individual test cases
     */
    private record SinglePercentEncoded(Arguments... entries) {}

    /**
     * This record should contain tests cases which are double percent-encoded. Double percent-encoding is a technique
     * whereby a percent-encoded URL is re-encoded. There are very few legitimate use-cases for using this technique,
     * but double-encoding is a popular URL vulnerability that we need to ensure we are safe against. What we would
     * hope to check is that our source-code is not doing a single decoding pass that would then leave it vulnerable
     * to double-encoding.
     * <p>
     * You can read more about double-encoding at
     * <a href="https://en.wikipedia.org/wiki/Double_encoding#Double_URI-encoding">Wikipedia: Double-Encoding</a> or
     * <a href="https://owasp.org/w-community/Double_Encoding">OWASP: Double-Encoding</a>.
     *
     * @param entries Represents individual test cases
     */
    private record DoublePercentEncoded(Arguments... entries) {}

    /**
     * This record should contain test cases which are triple-or-more percent-encoded. It is a generalisation of the
     * double-encoding technique whereby we make sure we aren't arbitrary just protecting ourselves against single and
     * double encoding only, and that instead our verification logic can handle any arbitrary depth of encoding.
     *
     * @param entries Represents individual test cases
     */
    private record TripleOrMorePercentEncoded(Arguments... entries) {}

    /**
     * This record should contain test cases which contain overlong-encoding. In UTF-8 one could theoretically express
     * the same unicode code point as a single-byte, or as a double/triple/quadruple byte sequence. However, UTF-8
     * specifies that you should always use the shortest form possible and that it is forbidden to use a longer form
     * than necessary. You can read more about overlong-encoding at
     * <a href="https://en.wikipedia.org/wiki/UTF-8#Overlong_encodings">Wikipedia: UTF-8 Overlong</a> or at
     * <a href="http://www.unicode.org/versions/corrigendum1.html">Unicode Corrigendum: UTF-8 Shortest Form</a>.
     * <p>
     * Despite being illegal, it is still a popular vulnerability because many pieces of software will decode overlong
     * UTF-8 byte sequences to unexpected values.
     * <p>
     * You will find that our tests decode any overlong values to the
     * <a href="https://www.fileformat.info/info/unicode/char/0fffd/index.htm">Unicode Replacement Character</a>.
     *
     * @param entries Represents individual test cases
     */
    private record OverlongPercentEncoded(Arguments... entries) {}

    /**
     * Utility class that helps us structure our test code in a manner that makes it obvious that we are providing
     * test cases of famous vulnerabilities.
     *
     * @param nonEncoded not-encoded test cases
     * @param singleEncoded single-percent-encoded test cases
     * @param doubleEncoded double-percent-encoded test cases
     * @param tripleOrMoreEncoded triple-or-more-percent-encoded test cases
     * @param overlongEncoded overlong-percent-encoded test cases
     * @return combined test cases
     */
    private static Stream<Arguments> buildTestCases(
            NonPercentEncoded nonEncoded,
            SinglePercentEncoded singleEncoded,
            DoublePercentEncoded doubleEncoded,
            TripleOrMorePercentEncoded tripleOrMoreEncoded,
            OverlongPercentEncoded overlongEncoded) {
        return Stream.concat(
                Stream.of(nonEncoded.entries),
                Stream.concat(
                        Stream.of(singleEncoded.entries),
                        Stream.concat(
                                Stream.of(doubleEncoded.entries),
                                Stream.concat(
                                        Stream.of(tripleOrMoreEncoded.entries), Stream.of(overlongEncoded.entries)))));
    }

    private static Arguments arg(ValidationStatus status, String location, String result) {
        return isWindows()
                ? ArgTransformerWindows.transform(status, location, result)
                : ArgTransformerIdentity.transform(status, location, result);
    }

    private static boolean isWindows() {
        return '\\' == File.separatorChar;
    }

    private static class ArgTransformerWindows {
        public static Arguments transform(ValidationStatus status, String location, String result) {
            if (status != OK) {
                return Arguments.of(status, location, null);
            } else if (urlContainsEncodedLeadingSlashes(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else if (urlContainsWindowsReservedCharactersInPath(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else if (urlContainsEncodedUnicodeC0RangeCharacters(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else {
                final var resultWithDrive = urlWithDefaultDrive(result);
                final var resultWitEncodedChars = urlWithPercentEncodedMultiByteChar(resultWithDrive);
                return Arguments.of(status, location, resultWitEncodedChars);
            }
        }

        /**
         * Windows doesn't normalise encoded leading slashes in the same way that other operating systems do. It will
         * throw an invalid Path exception instead. This utility method detects such cases.
         *
         * @param url candidate to check
         * @return whether url contains encoded leading slashes
         */
        private static boolean urlContainsEncodedLeadingSlashes(String url) {
            final var pattern = Pattern.compile("file:/+%2F.*");
            final var matcher = pattern.matcher(url);
            return matcher.matches();
        }

        /**
         * Windows doesn't allow certain reserved characters in its path. This utility method detects such characters.
         * <a href="https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions">Find out
         * more here</a>.
         *
         * @param url candidate to check
         * @return whether url contains Windows reserved characters
         */
        private static boolean urlContainsWindowsReservedCharactersInPath(String url) {
            try {
                final var pattern = Pattern.compile(".*(<|>|:|\"|\\|\\?|\\*|%3C|%3E|%3A|%22|%5C|%3F|%2A).*");
                final var matcher = pattern.matcher(new URL(url).getPath());
                return matcher.matches();
            } catch (MalformedURLException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        /**
         * Windows doesn't allow characters in the Unicode C0 range.
         * <a href="https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions">Find out
         * more here</a> and <a href="https://en.wikipedia.org/wiki/List_of_Unicode_characters#Control_codes">here</a>.
         * > (banned) Characters whose integer representations are in the range from 1 through 31.
         *
         * @param url candidate to check
         * @return whether url contains Unicode C0 range characters
         */
        private static boolean urlContainsEncodedUnicodeC0RangeCharacters(String url) {
            final var pattern = Pattern.compile(".*%[01][0-9A-F].*");
            final var matcher = pattern.matcher(url);
            return matcher.matches();
        }

        /**
         * The Path returned by Windows contains a URL with a drive. This utility method helps can transform a URL
         * into a URL with Windows' default drive.
         *
         * @param url input to transform
         * @return the url containing a Windows drive
         */
        private static String urlWithDefaultDrive(String url) {
            final var root =
                    GraphDatabaseSettings.neo4j_home.defaultValue().toString().substring(0, 2);
            return "file:/" + root + "/" + url.substring("file:/".length());
        }

        /**
         * For multibyte unicode code points, Unix and Mac leaves them percent-encoded as-is whereas Windows encodes
         * them to utf-8. This utility. method will convert the multibyte encoded characters we have in our test cases
         * to utf-8.
         *
         * @param url input to transform
         * @return the url with utf-8 encoded replacement characters
         */
        private static String urlWithPercentEncodedMultiByteChar(String url) {
            return url.replaceAll("%EF%BF%BD", "\uFFFD")
                    .replaceAll("%C2%A5", "\u00A5")
                    .replaceAll("%C2%A3", "\u00A3");
        }
    }

    private static class ArgTransformerIdentity {
        public static Arguments transform(ValidationStatus status, String location, String result) {
            return Arguments.of(status, location, result);
        }
    }
}
