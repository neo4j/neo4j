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
package org.neo4j.configuration.helpers;

import static java.lang.Integer.parseInt;
import static java.lang.String.format;

import java.net.URI;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketAddressParser {
    private static final Pattern hostnamePortPatternExt = Pattern.compile("\\[(?<hostname>[^\\s]+)]:(?<port>\\d+)");
    private static final Pattern hostnamePortPattern =
            Pattern.compile("(?<hostname>[^\\s]*([^:\\s]|::)):(?<port>\\d+)");
    // hostnamePattern matches if there are no colons (most usage) or if there are 2 or more colons (raw IPv6 address)
    // the lookahead needs to include the end of string anchor ($) to ensure it matches the whole string - although the
    // main regex does not need the trailing $
    private static final Pattern hostnamePattern =
            Pattern.compile("(?=([^:]+:?|[^:^\\s]*:[^:^\\s]*:[^\\s]*)$)(?<hostname>[^\\s]+)");
    private static final Pattern portPattern = Pattern.compile(":(?<port>\\d+)");

    public static <T extends SocketAddress> T deriveSocketAddress(
            String settingName,
            String settingValue,
            String defaultHostname,
            int defaultPort,
            BiFunction<String, Integer, T> constructor) {
        if (settingValue == null) {
            return constructor.apply(defaultHostname, defaultPort);
        }

        settingValue = settingValue.trim();

        T socketAddress;
        if ((socketAddress = matchHostnamePort(settingValue, constructor)) != null) {
            return socketAddress;
        }

        if ((socketAddress = matchPort(settingValue, defaultHostname, constructor)) != null) {
            return socketAddress;
        }

        throw new IllegalArgumentException(format(
                "Setting \"%s\" must be in the format of "
                        + "\"hostname:port\" or \":port\". \"%s\" does not conform to these formats",
                settingName, settingValue));
    }

    public static <T extends SocketAddress> T socketAddress(
            URI uri, int defaultPort, BiFunction<String, Integer, T> constructor) {
        int port = uri.getPort();
        return constructor.apply(uri.getHost(), port > 0 ? port : defaultPort);
    }

    public static <T extends SocketAddress> T socketAddress(
            String settingValue, BiFunction<String, Integer, T> constructor) {
        return socketAddress(settingValue, -1, constructor);
    }

    public static <T extends SocketAddress> T socketAddress(
            String settingValue, int defaultPort, BiFunction<String, Integer, T> constructor) {
        if (settingValue == null) {
            throw new IllegalArgumentException("Cannot parse socket address from null");
        }

        if (settingValue.contains("://")) {
            throw new IllegalArgumentException(format(
                    "Configured socket address seems to be a URI. The socket address must be in the format of "
                            + "\"hostname:port\", \"hostname\" or \":port\". \"%s\" does not conform to these formats",
                    settingValue));
        }

        settingValue = settingValue.trim();

        T socketAddress;
        if ((socketAddress = matchHostnamePort(settingValue, constructor)) != null) {
            return socketAddress;
        }

        if ((socketAddress = matchPort(settingValue, null, constructor)) != null) {
            return socketAddress;
        }

        if ((socketAddress = matchHostname(settingValue, defaultPort, constructor)) != null) {
            return socketAddress;
        }

        throw new IllegalArgumentException(format(
                "Configured socket address must be in the format of "
                        + "\"hostname:port\", \"hostname\" or \":port\". \"%s\" does not conform to these formats",
                settingValue));
    }

    private static <T extends SocketAddress> T matchHostname(
            String settingValue, int defaultPort, BiFunction<String, Integer, T> constructor) {
        Matcher hostnameMatcher = hostnamePattern.matcher(settingValue);
        if (hostnameMatcher.matches()) {
            String hostname = hostnameMatcher.group("hostname");
            return constructor.apply(hostname, defaultPort);
        }
        return null;
    }

    private static <T extends SocketAddress> T matchHostnamePort(
            String settingValue, BiFunction<String, Integer, T> constructor) {
        Matcher hostnamePortWithBracketsMatcher = hostnamePortPatternExt.matcher(settingValue);
        if (hostnamePortWithBracketsMatcher.matches()) {
            String hostname = hostnamePortWithBracketsMatcher.group("hostname");
            int port = parseInt(hostnamePortWithBracketsMatcher.group("port"));
            return constructor.apply(hostname, port);
        }

        Matcher hostnamePortMatcher = hostnamePortPattern.matcher(settingValue);
        if (hostnamePortMatcher.matches()) {
            String hostname = hostnamePortMatcher.group("hostname");
            int port = parseInt(hostnamePortMatcher.group("port"));
            return constructor.apply(hostname, port);
        }

        return null;
    }

    private static <T extends SocketAddress> T matchPort(
            String settingValue, String defaultHostname, BiFunction<String, Integer, T> constructor) {
        Matcher portMatcher = portPattern.matcher(settingValue);
        if (portMatcher.matches()) {
            int port = parseInt(portMatcher.group("port"));
            return constructor.apply(defaultHostname, port);
        }

        return null;
    }
}
