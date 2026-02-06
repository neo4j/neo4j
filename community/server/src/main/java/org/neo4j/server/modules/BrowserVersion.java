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
package org.neo4j.server.modules;

import java.nio.file.Path;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.Objects;
import java.util.regex.Pattern;

public record BrowserVersion(Path filePath, LocalDate versionDate, int subVersionNo)
        implements Comparable<BrowserVersion> {

    private static final Pattern BROWSER_PATTERN = Pattern.compile(
            "^neo4j-browser(-(?<year>\\d{4})\\.(?<month>0[1-9]|1[0-2])\\.(?<day>0[1-9]|[12]\\d|3[01])\\+(?<subversion>\\d+))?.zip$");

    public static BrowserVersion fromPath(Path path) throws ParseException {
        var fileName = path.getFileName().toString();
        var matcher = BROWSER_PATTERN.matcher(fileName);

        if (!matcher.matches()) {
            throw new ParseException("Invalid browser version format", 0);
        }

        if (matcher.group("year") == null) {
            return unversioned(path);
        }

        var date = LocalDate.of(
                Integer.parseInt(matcher.group("year")),
                Integer.parseInt(matcher.group("month")),
                Integer.parseInt(matcher.group("day")));

        var version = Integer.parseInt(matcher.group("subversion"));

        return new BrowserVersion(path, date, version);
    }

    public static BrowserVersion unversioned(Path path) {
        return new BrowserVersion(path, LocalDate.EPOCH, 0);
    }

    @Override
    public int compareTo(BrowserVersion other) {
        var dateComparison = versionDate.compareTo(other.versionDate);

        if (dateComparison != 0) {
            return dateComparison;
        }

        return Integer.compare(subVersionNo, other.subVersionNo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(versionDate, subVersionNo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BrowserVersion that = (BrowserVersion) obj;
        return subVersionNo == that.subVersionNo && versionDate.equals(that.versionDate);
    }
}
