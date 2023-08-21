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
package org.neo4j.kernel.impl.newapi.index;

class Range10IndexParams implements IndexParams {

    @Override
    public String providerKey() {
        return "range";
    }

    @Override
    public String providerVersion() {
        return "1.0";
    }

    @Override
    public boolean indexProvidesStringValues() {
        return true;
    }

    @Override
    public boolean indexProvidesNumericValues() {
        return true;
    }

    @Override
    public boolean indexProvidesAllValues() {
        return true;
    }

    @Override
    public boolean indexProvidesArrayValues() {
        return true;
    }

    @Override
    public boolean indexProvidesBooleanValues() {
        return true;
    }

    @Override
    public boolean indexProvidesSpatialValues() {
        return true;
    }

    @Override
    public boolean indexProvidesTemporalValues() {
        return true;
    }

    @Override
    public boolean indexSupportsStringSuffixAndContains() {
        return false;
    }
}
