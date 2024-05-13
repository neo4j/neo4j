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
package org.neo4j.kernel.impl.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_ARG;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_AUTH;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_FRAGMENT;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_PATH;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_QUERY;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.ERR_URI;
import static org.neo4j.kernel.impl.security.FileURIAccessRuleTest.ValidationStatus.OK;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.logging.NullLog;

class FileURIAccessRuleTest {

    private static final Pattern FILE_URI_PATTERN = Pattern.compile("file:///[^/].*");
    private static final Pattern LEADING_SLASHES_PATTERN = Pattern.compile("file:/+%2F.*");
    private static final Pattern WINDOWS_RESERVED_CHARS_PATTERN =
            Pattern.compile(".*(<|>|:|\"|\\|\\?|\\*|%3C|%3E|%3A|%22|%5C|%3F|%2A).*");
    private static final Pattern UNICODE_C0_RANGE_PATTERN = Pattern.compile(".*%[01][0-9A-F].*");

    private static final SecurityAuthorizationHandler AUTHORIZATION_HANDLER =
            new SecurityAuthorizationHandler(new CommunitySecurityLog(NullLog.getInstance()));

    @Test
    void shouldThrowWhenFileAccessIsDisabled() {
        final var errorMessage = "configuration property 'dbms.security.allow_csv_import_from_file_urls' is false";

        final var config = Config.defaults(GraphDatabaseSettings.allow_file_urls, false);
        assertThatThrownBy(() -> new FileURIAccessRule(config)
                        .validate(
                                URI.create("file:///dir/file.csv"),
                                AUTHORIZATION_HANDLER,
                                SecurityContext.AUTH_DISABLED))
                .isInstanceOf(URLAccessValidationError.class)
                .hasMessageContaining(errorMessage);

        assertThatThrownBy(() -> new FileURIAccessRule(config)
                        .validate(
                                URI.create("s3://some-bucket/file.csv"),
                                AUTHORIZATION_HANDLER,
                                SecurityContext.AUTH_DISABLED))
                .isInstanceOf(URLAccessValidationError.class)
                .hasMessageContaining(errorMessage);
    }

    @Test
    void shouldThrowWhenSchemeCannotBeResolved() {
        assertThatThrownBy(() -> new FileURIAccessRule(Config.defaults())
                        .getReader(
                                URI.create("boom://dir/file.csv"),
                                AUTHORIZATION_HANDLER,
                                SecurityContext.AUTH_DISABLED))
                .isInstanceOf(URLAccessValidationError.class)
                .hasMessageContaining("Invalid URL 'boom://dir/file.csv': unknown protocol: boom");
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
                        arg(ERR_QUERY, "s3://some-bucket?csv", null),
                        arg(ERR_QUERY, "s3://some-bucket/?csv", null),
                        arg(ERR_ARG, "file:/?", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%3Fcsv", "file:///import/file%3Fcsv"),
                        arg(OK, "file:/%3Fcsv", "file:///import/%3Fcsv"),
                        arg(OK, "file:///%3F", "file:///import/%3F"),
                        arg(OK, "file:/%3F", "file:///import/%3F")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%253Fcsv", "file:///import/file%253Fcsv"),
                        arg(OK, "file:/%253Fcsv", "file:///import/%253Fcsv"),
                        arg(OK, "file:///%253F", "file:///import/%253F"),
                        arg(OK, "file:/%253F", "file:///import/%253F")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%25253Fcsv", "file:///import/file%25253Fcsv"),
                        arg(OK, "file:/file%2525253Fcsv", "file:///import/file%2525253Fcsv"),
                        arg(OK, "file:/file%252525253Fcsv", "file:///import/file%252525253Fcsv")));
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
                        arg(OK, "file:/file%23csv", "file:///import/file%23csv"),
                        arg(OK, "file:/%23csv", "file:///import/%23csv"),
                        arg(OK, "file:///%23csv", "file:///import/%23csv"),
                        arg(OK, "file:/%23", "file:///import/%23")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%2523csv", "file:///import/file%2523csv"),
                        arg(OK, "file:/%2523csv", "file:///import/%2523csv"),
                        arg(OK, "file:///%2523csv", "file:///import/%2523csv"),
                        arg(OK, "file:/%2523", "file:///import/%2523")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%252523csv", "file:///import/file%252523csv"),
                        arg(OK, "file:/file%25252523csv", "file:///import/file%25252523csv"),
                        arg(OK, "file:/file%2525252523csv", "file:///import/file%2525252523csv")));
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
                        arg(OK, "file:/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv", "file:///import/file.csv"),
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
                        arg(OK, "file:/%2Ffile.csv", "file:///import/file.csv"),
                        arg(ERR_ARG, "file:%2F/file.csv", null),
                        arg(ERR_ARG, "file:%2F%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F%2F//file.csv", null),
                        arg(ERR_AUTH, "file://%2Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2F///////file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252F%252Ffile.csv", "file:///import/%252F%252Ffile.csv"),
                        arg(ERR_ARG, "file:%252F%252F%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%252F//%2EF////file.csv", null),
                        arg(ERR_AUTH, "file://%252Efile.csv", null),
                        arg(ERR_AUTH, "file://%252E%252Efile.csv", null),
                        arg(ERR_AUTH, "file://%253A/file.csv", null),
                        arg(ERR_AUTH, "file://me.com/file%252Ecsv", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252F%252Ffile.csv", "file:///import/%25252F%252Ffile.csv"),
                        arg(ERR_ARG, "file:%25252F%252F%252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%25252Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2525252Efile.csv", null),
                        arg(ERR_AUTH, "file://%2525252E%25252Efile.csv", null)));
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
                        arg(OK, "file:/:file.csv", "file:///import/:file.csv"),
                        arg(OK, "file:/:80/file.csv", "file:///import/:80/file.csv"),
                        arg(OK, "file:/..:/file.csv", "file:///import/..:/file.csv"),
                        arg(OK, "file:/localhost/file.csv", "file:///import/localhost/file.csv"),
                        arg(OK, "file:/localhost:80/file.csv", "file:///import/localhost:80/file.csv"),
                        arg(OK, "file:/me.com/file.csv", "file:///import/me.com/file.csv"),
                        arg(OK, "file:/256.1.1.25/file.csv", "file:///import/256.1.1.25/file.csv"),
                        arg(OK, "file:/256.1.1.25:80/file.csv", "file:///import/256.1.1.25:80/file.csv"),
                        arg(OK, "file:/w.me.com:80/file.csv", "file:///import/w.me.com:80/file.csv"),
                        arg(OK, "file:///w.me.com:80/file.csv", "file:///import/w.me.com:80/file.csv"),
                        arg(OK, "file:///:file.csv", "file:///import/:file.csv"),
                        arg(OK, "file:/:/file.csv", "file:///import/:/file.csv"),
                        arg(OK, "file:/:/:file.csv", "file:///import/:/:file.csv"),
                        arg(OK, "file:///:/file.csv", "file:///import/:/file.csv"),
                        arg(OK, "file:/::file.csv", "file:///import/::file.csv"),
                        arg(OK, "file:///:::file.csv", "file:///import/:::file.csv"),
                        arg(OK, "s3://some-bucket/file.csv", "s3://some-bucket/file.csv"),
                        arg(ERR_ARG, "file::/file.csv", null),
                        arg(ERR_AUTH, "file://:file.csv", "file://:file.csv"),
                        arg(ERR_AUTH, "file://::file.csv", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2Efile.csv", "file:///import/.file.csv"),
                        arg(OK, "file:/%2E%2Efile.csv", "file:///import/..file.csv"),
                        arg(OK, "file:/%3Afile.csv", "file:///import/:file.csv"),
                        arg(OK, "file:/%3A/file.csv", "file:///import/:/file.csv"),
                        arg(OK, "file:///%3Afile.csv", "file:///import/:file.csv"),
                        arg(OK, "file:/:%3A:file.csv", "file:///import/:::file.csv"),
                        arg(OK, "file:/%3A:file.csv", "file:///import/::file.csv"),
                        arg(OK, "file:/%3A80%2Ffile.csv", "file:///import/:80/file.csv"),
                        arg(OK, "file:/localhost%3A80/file.csv", "file:///import/localhost:80/file.csv"),
                        arg(OK, "file:/me%2Ecom%3A80/file.csv", "file:///import/me.com:80/file.csv"),
                        arg(OK, "file:/%5B1%3A1:1:1:1:1%3A1:1%5D/csv", "file:///import/%5B1:1:1:1:1:1:1:1%5D/csv"),
                        arg(OK, "file:/%5B1:1:1:1:1:1:1:1%5D%3A80/c", "file:///import/%5B1:1:1:1:1:1:1:1%5D:80/c"),
                        arg(ERR_AUTH, "file://%3Afile.csv", null),
                        arg(ERR_AUTH, "file://%3A%3Afile.csv", null),
                        arg(ERR_ARG, "file%3A%3A/file.csv", null),
                        arg(ERR_ARG, "file%3A/file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/w%252Eme.com%253A80/csv", "file:///import/w%252Eme.com%253A80/csv"),
                        arg(OK, "file:///w.me%252Ecom:%252Fcsv", "file:///import/w.me%252Ecom:%252Fcsv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/w%25252Eme.com%25253A80/csv", "file:///import/w%25252Eme.com%25253A80/csv"),
                        arg(OK, "file:///w.me.com%2525253A80/csv", "file:///import/w.me.com%2525253A80/csv")));
    }

    /**
     * The business logic allows paths which are empty, although there isn't a use case for this.
     *
     * @return test cases
     */
    private static Stream<Arguments> pathsWhichAreEmpty() {
        return buildTestCases(
                new NonPercentEncoded(
                        arg(OK, "file:/", "file:///import"),
                        arg(OK, "file:///", "file:///import"),
                        arg(ERR_ARG, "file", null),
                        arg(ERR_URI, "file:", null)),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2F%2F", "file:///import"),
                        arg(ERR_ARG, "file:%2F", null),
                        arg(ERR_ARG, "file%3A", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252F%252F", "file:///import/%252F%252F"),
                        arg(ERR_ARG, "file:%252F", null),
                        arg(ERR_ARG, "file%253A", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%2525252F%252F", "file:///import/%2525252F%252F"),
                        arg(ERR_ARG, "file:%25252F", null),
                        arg(ERR_ARG, "file%25253A", null)));
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
                        arg(OK, "file:/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///////file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/dir1/file.csv", "file:///import/dir1/file.csv"),
                        arg(OK, "file:///dir1/file.csv", "file:///import/dir1/file.csv"),
                        arg(OK, "file:////dir1/file.csv", "file:///import/dir1/file.csv"),
                        arg(OK, "file://///////dir1/file.csv", "file:///import/dir1/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:///%2F//%2F///file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F////%2F/////file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2F%2F%2F%2F%2F%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:///%2F%2F%2F%2F%2F%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(ERR_ARG, "file:%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F//file.csv", null),
                        arg(ERR_ARG, "file:%2F/%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2F%2F/file.csv", null),
                        arg(ERR_ARG, "file:%2F%2F%2Ffile.csv", null),
                        arg(ERR_ARG, "file:%2Fdir1/file.csv", null),
                        arg(ERR_AUTH, "file://%2Ffile.csv", null),
                        arg(ERR_AUTH, "file://%2F///////file.csv", null)),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252Ffile%252Ecsv", "file:///import/%252Ffile%252Ecsv"),
                        arg(OK, "file:/%252Ffile%2520csv", "file:///import/%252Ffile%2520csv"),
                        arg(OK, "file:/%252Ffile.csv", "file:///import/%252Ffile.csv"),
                        arg(OK, "file:///%252Ffile.csv", "file:///import/%252Ffile.csv"),
                        arg(ERR_ARG, "file:%252F//file.csv", null)),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252Ffile%2520csv", "file:///import/%25252Ffile%2520csv"),
                        arg(OK, "file:/%25252Ffile%25252520csv", "file:///import/%25252Ffile%25252520csv"),
                        arg(OK, "file:/%25252Ffile%20csv", "file:///import/%25252Ffile%20csv"),
                        arg(ERR_ARG, "file:%25252F//file.csv", null)));
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
                        arg(OK, "file:/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/file.csv/", "file:///import/file.csv"),
                        arg(OK, "file:/file.csv//", "file:///import/file.csv"),
                        arg(OK, "file:/file.csv///", "file:///import/file.csv"),
                        arg(OK, "file:/file.csv////", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv/", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv//", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv///", "file:///import/file.csv"),
                        arg(OK, "file:///file.csv////", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv/", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv//", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv///", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv////", "file:///import/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/file.csv%2F", "file:///import/file.csv"),
                        arg(OK, "file:/file.csv%2F%2F%2F%2F", "file:///import/file.csv"),
                        // full URIs mean that the last / is retained unlike file:/file.csv%2f where it gets stripped
                        // for compatability with the Java File creation
                        arg(OK, "file:///file.csv%2F", "file:///import/file.csv/"),
                        arg(OK, "file:///file.csv%2F%2F%2F%2F%2F", "file:///import/file.csv/"),
                        arg(OK, "file:/%2F%2Ffile.csv%2F", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2Ffile.csv%2F%2F%2F%2F%2F", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv%2F%2F", "file:///import/file.csv"),
                        arg(OK, "file:////file.csv%2F%2F%2F%2F", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2F%2Ffile.csv//", "file:///import/file.csv"),
                        arg(OK, "file:/%2F%2F%2Ffile.csv%2F%2F%2F%2F", "file:///import/file.csv"),
                        arg(OK, "file://///%2F%2F%2Ffile.csv//", "file:///import/file.csv"),
                        arg(OK, "file://///%2F%2F%2Ffile.csv%2F%2F%2F%2F", "file:///import/file.csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file.csv%252F", "file:///import/file.csv%252F"),
                        arg(OK, "file:/file.csv%252F%2F%2F%2F", "file:///import/file.csv%252F"),
                        arg(OK, "file:///file.csv%252F", "file:///import/file.csv%252F"),
                        arg(OK, "file:/%252F%252Ffile.csv%252F", "file:///import/%252F%252Ffile.csv%252F"),
                        arg(OK, "file:////file.csv%252F%252F", "file:///import/file.csv%252F%252F"),
                        arg(OK, "file:/%252F%252F%252Ffile.csv//", "file:///import/%252F%252F%252Ffile.csv"),
                        arg(OK, "file://///%252F%252F%252Ffile.csv//", "file:///import/%252F%252F%252Ffile.csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file.csv%25252F", "file:///import/file.csv%25252F"),
                        arg(OK, "file:/file.csv%2525252F%252F", "file:///import/file.csv%2525252F%252F"),
                        arg(OK, "file:/%25252F%25252Ffile.csv", "file:///import/%25252F%25252Ffile.csv"),
                        arg(OK, "file:////file.csv%25252F%25252F", "file:///import/file.csv%25252F%25252F"),
                        arg(OK, "file:/%25252Ffile.csv%25252F", "file:///import/%25252Ffile.csv%25252F"),
                        arg(OK, "file://///%25252F%25252Ffile.csv//", "file:///import/%25252F%25252Ffile.csv")));
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
                        arg(OK, "file:/.file.csv", "file:///import/.file.csv"),
                        arg(OK, "file:/..file.csv", "file:///import/..file.csv"),
                        arg(OK, "file:/...file.csv", "file:///import/...file.csv"),
                        arg(OK, "file:///.file.csv", "file:///import/.file.csv"),
                        arg(OK, "file:///..file.csv", "file:///import/..file.csv"),
                        arg(OK, "file:///...file.csv", "file:///import/...file.csv"),
                        arg(OK, "file:/./file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/~file.csv", "file:///import/~file.csv"),
                        arg(OK, "file:/~/file.csv", "file:///import/~/file.csv"),
                        arg(OK, "file:/../../~file.csv", "file:///import/~file.csv"),
                        arg(OK, "file:/../~/../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///./file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/dir1/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///dir1/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/..//dir1/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/../../dir1/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///import/../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/../import/file.csv", "file:///import/import/file.csv"),
                        arg(OK, "file:///../import/file.csv", "file:///import/import/file.csv"),
                        arg(OK, "file://///////../import/file.csv", "file:///import/import/file.csv"),
                        arg(OK, "file:/..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/..//..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///..//..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//..//..//../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//..//../..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////../..//../..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////../../../..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////../../../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//../..//../../file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..//..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file://////..//..//..//..//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/..///..///..///file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..///file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..///..///file.csv", "file:///import/file.csv"),
                        arg(OK, "file://///..///..///..///file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////.//.//file.csv", "file:///import/file.csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/%2E%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///%2E%2E%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E%2E/%2E%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E%2E/%2E%2E/%2E%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E%2E%2F%2E%2E%2F%2E%2E%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E%2E%2Fimport/file.csv", "file:///import/import/file.csv"),
                        arg(OK, "file:/%2efile.csv", "file:///import/.file.csv"),
                        arg(OK, "file:/%2Efile.csv", "file:///import/.file.csv"),
                        arg(OK, "file:/%2Efile%2Ecsv", "file:///import/.file.csv"),
                        arg(OK, "file:///%2Efile.csv", "file:///import/.file.csv"),
                        arg(OK, "file:/%7Efile.csv", "file:///import/~file.csv"),
                        arg(OK, "file:/%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:///%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/../%7E/../file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E/%2E/%2E/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E/%2E/%2E/%2Efile.csv", "file:///import/.file.csv"),
                        arg(ERR_AUTH, "file://%2Efile.csv", null),
                        arg(ERR_AUTH, "file://%2E%2Efile.csv", null),
                        arg(OK, "file:/%2E%2E%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:/%2E./file.csv", "file:///import/file.csv"),
                        arg(OK, "file:/..%2Ffile.csv", "file:///import/file.csv"),
                        // combinations of file:////..//..//file.csv
                        arg(OK, "file:/%2e%2e//%2e%2e//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..//..%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:////../%2F..%2F/file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////%2e%2e//%2e%2e//file.csv", "file:///import/file.csv"),
                        arg(OK, "file:////..%2F%2F..%2F%2Ffile.csv", "file:///import/file.csv"),
                        arg(OK, "file:////%2E%2E%2F%2F%2E%2E%2F%2Ffile.csv", "file:///import/file.csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/%252E%252E/file.csv", "file:///import/%252E%252E/file.csv"),
                        arg(OK, "file:///%252E%252E%252Ffile.csv", "file:///import/%252E%252E%252Ffile.csv"),
                        arg(OK, "file:/%252E%252E/%252E/file.csv", "file:///import/%252E%252E/%252E/file.csv"),
                        arg(OK, "file:/%252E%252E%252Fimport/csv", "file:///import/%252E%252E%252Fimport/csv"),
                        arg(OK, "file:////..//..%252F%2Ffile.csv", "file:///import/..%252F/file.csv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/%25252E%25252E/file.csv", "file:///import/%25252E%25252E/file.csv"),
                        arg(OK, "file:///%2525252E%25252Ffile.csv", "file:///import/%2525252E%25252Ffile.csv"),
                        arg(OK, "file:////..//..%25252F%2Ffile.csv", "file:///import/..%25252F/file.csv")));
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
                        arg(OK, "file:/file:csv", "file:///import/file:csv"),
                        arg(OK, "file:/file/csv", "file:///import/file/csv"),
                        arg(ERR_QUERY, "file:/file?csv", null),
                        arg(ERR_FRAGMENT, "file:/file#csv", null),
                        arg(OK, "file:/file@csv", "file:///import/file@csv"),
                        arg(OK, "file:/file!csv", "file:///import/file!csv"),
                        arg(OK, "file:/file$csv", "file:///import/file$csv"),
                        arg(OK, "file:/file&csv", "file:///import/file&csv"),
                        arg(OK, "file:/file'csv", "file:///import/file'csv"),
                        arg(OK, "file:/file(csv", "file:///import/file(csv"),
                        arg(OK, "file:/file)csv", "file:///import/file)csv"),
                        arg(OK, "file:/file*csv", "file:///import/file*csv"),
                        arg(OK, "file:/file+csv", "file:///import/file+csv"),
                        arg(OK, "file:/file,csv", "file:///import/file,csv"),
                        arg(OK, "file:/file;csv", "file:///import/file;csv"),
                        arg(OK, "file:/file=csv", "file:///import/file=csv"),
                        arg(ERR_URI, "file:/file[csv", null),
                        arg(ERR_URI, "file:/file]csv", null),
                        arg(ERR_URI, "file:/file csv", null)),
                new SinglePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%25csv", "file:///import/file%25csv"),
                        arg(OK, "file:/file%3Acsv", "file:///import/file:csv"),
                        arg(OK, "file:/file%2Fcsv", "file:///import/file/csv"),
                        arg(OK, "file:/file%3Fcsv", "file:///import/file%3Fcsv"),
                        arg(OK, "file:/file%23csv", "file:///import/file%23csv"),
                        arg(OK, "file:/file%40csv", "file:///import/file@csv"),
                        arg(OK, "file:/file%21csv", "file:///import/file!csv"),
                        arg(OK, "file:/file%24csv", "file:///import/file$csv"),
                        arg(OK, "file:/file%26csv", "file:///import/file&csv"),
                        arg(OK, "file:/file%27csv", "file:///import/file'csv"),
                        arg(OK, "file:/file%28csv", "file:///import/file(csv"),
                        arg(OK, "file:/file%29csv", "file:///import/file)csv"),
                        arg(OK, "file:/file%2Acsv", "file:///import/file*csv"),
                        arg(OK, "file:/file%2Bcsv", "file:///import/file+csv"),
                        arg(OK, "file:/file%2Ccsv", "file:///import/file,csv"),
                        arg(OK, "file:/file%3Bcsv", "file:///import/file;csv"),
                        arg(OK, "file:/file%3Dcsv", "file:///import/file=csv"),
                        arg(OK, "file:/file%5Bcsv", "file:///import/file%5Bcsv"),
                        arg(OK, "file:/file%5Dcsv", "file:///import/file%5Dcsv"),
                        arg(OK, "file:/file%20csv", "file:///import/file%20csv")),
                new DoublePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%2525csv", "file:///import/file%2525csv"),
                        arg(OK, "file:/file%253Acsv", "file:///import/file%253Acsv"),
                        arg(OK, "file:/file%252Fcsv", "file:///import/file%252Fcsv"),
                        arg(OK, "file:/file%253Fcsv", "file:///import/file%253Fcsv"),
                        arg(OK, "file:/file%2523csv", "file:///import/file%2523csv"),
                        arg(OK, "file:/file%2540csv", "file:///import/file%2540csv"),
                        arg(OK, "file:/file%2521csv", "file:///import/file%2521csv"),
                        arg(OK, "file:/file%2524csv", "file:///import/file%2524csv"),
                        arg(OK, "file:/file%2526csv", "file:///import/file%2526csv"),
                        arg(OK, "file:/file%2527csv", "file:///import/file%2527csv"),
                        arg(OK, "file:/file%2528csv", "file:///import/file%2528csv"),
                        arg(OK, "file:/file%2529csv", "file:///import/file%2529csv"),
                        arg(OK, "file:/file%252Acsv", "file:///import/file%252Acsv"),
                        arg(OK, "file:/file%252Bcsv", "file:///import/file%252Bcsv"),
                        arg(OK, "file:/file%252Ccsv", "file:///import/file%252Ccsv"),
                        arg(OK, "file:/file%253Bcsv", "file:///import/file%253Bcsv"),
                        arg(OK, "file:/file%253Dcsv", "file:///import/file%253Dcsv"),
                        arg(OK, "file:/file%255Bcsv", "file:///import/file%255Bcsv"),
                        arg(OK, "file:/file%255Dcsv", "file:///import/file%255Dcsv"),
                        arg(OK, "file:/file%2520csv", "file:///import/file%2520csv")),
                new TripleOrMorePercentEncoded(
                        // in the same order as above
                        arg(OK, "file:/file%252525csv", "file:///import/file%252525csv"),
                        arg(OK, "file:/file%25253Acsv", "file:///import/file%25253Acsv"),
                        arg(OK, "file:/file%25252Fcsv", "file:///import/file%25252Fcsv"),
                        arg(OK, "file:/file%25253Fcsv", "file:///import/file%25253Fcsv"),
                        arg(OK, "file:/file%252523csv", "file:///import/file%252523csv"),
                        arg(OK, "file:/file%252540csv", "file:///import/file%252540csv"),
                        arg(OK, "file:/file%252521csv", "file:///import/file%252521csv"),
                        arg(OK, "file:/file%252524csv", "file:///import/file%252524csv"),
                        arg(OK, "file:/file%252526csv", "file:///import/file%252526csv"),
                        arg(OK, "file:/file%252527csv", "file:///import/file%252527csv"),
                        arg(OK, "file:/file%252528csv", "file:///import/file%252528csv"),
                        arg(OK, "file:/file%252529csv", "file:///import/file%252529csv"),
                        arg(OK, "file:/file%25252Acsv", "file:///import/file%25252Acsv"),
                        arg(OK, "file:/file%25252Bcsv", "file:///import/file%25252Bcsv"),
                        arg(OK, "file:/file%25252Ccsv", "file:///import/file%25252Ccsv"),
                        arg(OK, "file:/file%25253Bcsv", "file:///import/file%25253Bcsv"),
                        arg(OK, "file:/file%25253Dcsv", "file:///import/file%25253Dcsv"),
                        arg(OK, "file:/file%25255Bcsv", "file:///import/file%25255Bcsv"),
                        arg(OK, "file:/file%25255Dcsv", "file:///import/file%25255Dcsv"),
                        arg(OK, "file:/file%252520csv", "file:///import/file%252520csv")));
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
                        arg(ERR_URI, "file:/file csv", null), arg(OK, "file:/file+csv", "file:///import/file+csv")),
                new SinglePercentEncoded(
                        arg(OK, "file:/file%20csv", "file:///import/file%20csv"),
                        arg(OK, "file:/file%2Bcsv", "file:///import/file+csv")),
                new DoublePercentEncoded(
                        arg(OK, "file:/file%2520csv", "file:///import/file%2520csv"),
                        arg(OK, "file:/file%252Bcsv", "file:///import/file%252Bcsv")),
                new TripleOrMorePercentEncoded(
                        arg(OK, "file:/file%252520csv", "file:///import/file%252520csv"),
                        arg(OK, "file:/file%25252Bcsv", "file:///import/file%25252Bcsv")));
    }

    private static Stream<Arguments> charactersEscapedLowerCase() {
        return Stream.of(
                arg(OK, "file:/file%2ecsv", "file:///import/file.csv"),
                arg(OK, "file:/file%2fcsv", "file:///import/file/csv"));
    }

    private static Stream<Arguments> charactersEscapedUpperCase() {
        return Stream.of(
                arg(OK, "file:/file%2Ecsv", "file:///import/file.csv"),
                arg(OK, "file:/file%2Fcsv", "file:///import/file/csv"));
    }

    protected enum ValidationStatus {
        OK, // Valid URL
        ERR_AUTH, // Invalid URL because it contains an authority
        ERR_QUERY, // Invalid URL because it contains a query string
        ERR_FRAGMENT, // Invalid URL because it contains a query fragment
        ERR_URI, // Syntactic error because URL can't be converted to URI
        ERR_PATH, // Syntactic error because URL can't be converted to Path
        ERR_ARG, // Syntactic error because URL can't be created from String
    }

    private void testValidation(ValidationStatus status, String root, String uri, String expected) throws Exception {
        if (status.equals(OK)) {
            URI accessURI = validate(root, uri);
            assertThat(accessURI.toString()).isEqualTo(expected);
        } else if (status.equals(ERR_AUTH)) {
            assertThatThrownBy(() -> validate(root, uri))
                    .isInstanceOf(URLAccessValidationError.class)
                    .hasMessageContaining("URL may not contain an authority section");
        } else if (status.equals(ERR_QUERY)) {
            assertThatThrownBy(() -> validate(root, uri))
                    .isInstanceOf(URLAccessValidationError.class)
                    .hasMessageContaining("file URL may not contain a query component");
        } else if (status.equals(ERR_FRAGMENT)) {
            assertThatThrownBy(() -> validate(root, uri))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("URI has a fragment component");
        } else if (status.equals(ERR_ARG)) {
            assertThatThrownBy(() -> validate(root, uri)).isInstanceOf(IllegalArgumentException.class);
        } else if (status.equals(ERR_PATH)) {
            assertThatThrownBy(() -> validate(root, uri)).isInstanceOf(InvalidPathException.class);
        } else if (status.equals(ERR_URI)) {
            // validate should throw either a URISyntaxException,
            // or a RuntimeException whose cause is a URISyntaxException
            assertThatThrownBy(() -> validate(root, uri))
                    .satisfiesAnyOf(
                            err -> assertThat(err)
                                    .isInstanceOf(RuntimeException.class)
                                    .hasCauseExactlyInstanceOf(URISyntaxException.class),
                            err -> assertThat(err).isInstanceOf(URISyntaxException.class));
        } else {
            throw new Exception("Unexpected test result");
        }
    }

    private URI validate(String root, String uri) throws URLAccessValidationError, URISyntaxException {
        final Config config = Config.defaults(GraphDatabaseSettings.load_csv_file_url_root, Path.of(root));
        return new FileURIAccessRule(config)
                .validate(new URI(uri), AUTHORIZATION_HANDLER, SecurityContext.AUTH_DISABLED);
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
     * Utility class that helps us structure our test code in a manner that makes it obvious that we are providing
     * test cases of famous vulnerabilities.
     *
     * @param nonEncoded not-encoded test cases
     * @param singleEncoded single-percent-encoded test cases
     * @param doubleEncoded double-percent-encoded test cases
     * @param tripleOrMoreEncoded triple-or-more-percent-encoded test cases
     * @return combined test cases
     */
    private static Stream<Arguments> buildTestCases(
            NonPercentEncoded nonEncoded,
            SinglePercentEncoded singleEncoded,
            DoublePercentEncoded doubleEncoded,
            TripleOrMorePercentEncoded tripleOrMoreEncoded) {
        return Stream.concat(
                Stream.of(nonEncoded.entries),
                Stream.concat(
                        Stream.of(singleEncoded.entries),
                        Stream.concat(Stream.of(doubleEncoded.entries), Stream.of(tripleOrMoreEncoded.entries))));
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
            } else if (uriContainsEncodedLeadingSlashes(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else if (uriContainsWindowsReservedCharactersInPath(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else if (uriContainsEncodedUnicodeC0RangeCharacters(location)) {
                return Arguments.of(ERR_PATH, location, null);
            } else {
                if (uriIsS3Based(location)) {
                    return Arguments.of(status, location, result);
                }

                var resultUri = uriWithDefaultDrive(result);
                if (uriContainsEncodedTrailingSlashes(location)) {
                    // need to strip that trailing slash for the special windows case
                    return Arguments.of(status, location, resultUri.substring(0, resultUri.length() - 1));
                } else {
                    return Arguments.of(status, location, resultUri);
                }
            }
        }

        private static boolean uriIsS3Based(String uri) {
            return uri.startsWith("s3://");
        }

        /**
         * Windows doesn't normalise encoded leading slashes in the same way that other operating systems do. It will
         * throw an invalid Path exception instead. This utility method detects such cases.
         *
         * @param uri candidate to check
         * @return whether url contains encoded leading slashes
         */
        private static boolean uriContainsEncodedLeadingSlashes(String uri) {
            return LEADING_SLASHES_PATTERN.matcher(uri).matches();
        }

        /**
         * In <code>file:///file.csv%2f</code>, the terminal <code>/</code> gets stripped for compatability with the
         * Java {@link File} creation
         * @param uri candidate to check
         * @return if the terminal <code>/</code> should be removed
         */
        private static boolean uriContainsEncodedTrailingSlashes(String uri) {
            return uri.endsWith("%2F") && FILE_URI_PATTERN.matcher(uri).matches();
        }

        /**
         * Windows doesn't allow certain reserved characters in its path. This utility method detects such characters.
         * <a href="https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions">Find out
         * more here</a>.
         *
         * @param uri candidate to check
         * @return whether url contains Windows reserved characters
         */
        private static boolean uriContainsWindowsReservedCharactersInPath(String uri) {
            try {
                return WINDOWS_RESERVED_CHARS_PATTERN
                        .matcher(new URI(uri).getRawPath())
                        .matches();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e.getMessage());
            }
        }

        /**
         * Windows doesn't allow characters in the Unicode C0 range.
         * <a href="https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions">Find out
         * more here</a> and <a href="https://en.wikipedia.org/wiki/List_of_Unicode_characters#Control_codes">here</a>.
         * > (banned) Characters whose integer representations are in the range from 1 through 31.
         *
         * @param uri candidate to check
         * @return whether url contains Unicode C0 range characters
         */
        private static boolean uriContainsEncodedUnicodeC0RangeCharacters(String uri) {
            return UNICODE_C0_RANGE_PATTERN.matcher(uri).matches();
        }

        /**
         * The Path returned by Windows contains a URI with a drive. This utility method helps can transform a URI
         * into a URI with Windows' default drive.
         *
         * @param uri input to transform
         * @return the url containing a Windows drive
         */
        private static String uriWithDefaultDrive(String uri) {
            final var root =
                    GraphDatabaseSettings.neo4j_home.defaultValue().toString().substring(0, 2);
            return "file:///" + root + "/" + uri.substring("file:///".length());
        }
    }

    private static class ArgTransformerIdentity {
        public static Arguments transform(ValidationStatus status, String location, String result) {
            return Arguments.of(status, location, result);
        }
    }
}
