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

import java.util.Objects;

public class SettingSegment implements Segment {
    private final String setting;

    public SettingSegment(String setting) {
        this.setting = setting;
    }

    public String getSetting() {
        return setting;
    }

    @Override
    public boolean satisfies(Segment segment) {
        if (segment instanceof SettingSegment other) {
            return setting == null || setting.equals(other.setting);
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SettingSegment that = (SettingSegment) o;

        return Objects.equals(setting, that.setting);
    }

    @Override
    public int hashCode() {
        return setting != null ? setting.hashCode() : 0;
    }

    @Override
    public String toString() {
        return setting == null ? "*" : setting;
    }

    public static final SettingSegment ALL = new SettingSegment(null);
}
