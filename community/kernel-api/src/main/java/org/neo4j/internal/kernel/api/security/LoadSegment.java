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
package org.neo4j.internal.kernel.api.security;

// Cannot have private constructor as a record
// Want constructor to be private to only allow creating LoadSegments
// using the static variables/methods: ALL, CIDR, URL
@SuppressWarnings("ClassCanBeRecord")
public class LoadSegment implements Segment {
    private final String cidr, url;

    private LoadSegment(String cidr, String url) {
        this.cidr = cidr;
        this.url = url;
    }

    @Override
    public boolean satisfies(Segment segment) {
        if (segment instanceof LoadSegment other) {
            return (cidr == null || cidr.equals(other.cidr)) && (url == null || url.equals(other.url));
        }
        return false;
    }

    public static LoadSegment ALL = new LoadSegment(null, null);

    public static LoadSegment CIDR(String cidr) {
        return new LoadSegment(cidr, null);
    }

    public static LoadSegment URL(String url) {
        return new LoadSegment(null, url);
    }

    public static LoadSegment fromValueString(String value) {
        if (value.equals("ALL DATA")) {
            return ALL;
        } else if (value.startsWith("CIDR")) {
            // Will be split into: CIDR(, <range>, )
            String[] splitValue = value.split("'");
            return CIDR(splitValue[1]);
        } else {
            // Will be split into: URL(, <url>, )
            String[] splitValue = value.split("'");
            return URL(splitValue[1]);
        }
    }

    public boolean isCidr() {
        return cidr != null;
    }

    public boolean isUrl() {
        return url != null;
    }

    public String getCidr() {
        return cidr;
    }

    public String getUrl() {
        return url;
    }

    @Override
    public String toString() {
        if (cidr == null && url == null) {
            // Both null -> LOAD ON ALL DATA
            return "ALL DATA";
        } else if (cidr == null) {
            // cidr null -> LOAD ON URL
            return String.format("URL \"%s\"", url);
        } else {
            // url null -> LOAD ON CIDR
            return String.format("CIDR \"%s\"", cidr);
        }
    }

    public String toValue() {
        if (cidr == null && url == null) {
            return "ALL DATA";
        } else if (cidr == null) {
            return String.format("URL('%s')", url);
        } else {
            return String.format("CIDR('%s')", cidr);
        }
    }
}
