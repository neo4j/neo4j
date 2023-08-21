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
package org.neo4j.server.logging.slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.logging.log4j.MarkerManager;
import org.slf4j.IMarkerFactory;
import org.slf4j.Marker;

class SLF4JToLog4jMarker implements Marker {
    private final IMarkerFactory markerFactory;
    private final org.apache.logging.log4j.Marker log4jMarker;

    SLF4JToLog4jMarker(IMarkerFactory markerFactory, org.apache.logging.log4j.Marker log4jMarker) {
        this.markerFactory = markerFactory;
        this.log4jMarker = log4jMarker;
    }

    @Override
    public void add(Marker slf4jMarker) {
        if (slf4jMarker == null) {
            throw new IllegalArgumentException();
        }
        Marker m = markerFactory.getMarker(slf4jMarker.getName());
        log4jMarker.addParents(((SLF4JToLog4jMarker) m).getLog4jMarker());
    }

    @Override
    public boolean contains(Marker other) {
        if (other == null) {
            throw new IllegalArgumentException();
        }
        return log4jMarker.isInstanceOf(other.getName());
    }

    @Override
    public boolean contains(String name) {
        return name != null && log4jMarker.isInstanceOf(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof SLF4JToLog4jMarker other)) {
            return false;
        }
        return Objects.equals(log4jMarker, other.log4jMarker);
    }

    org.apache.logging.log4j.Marker getLog4jMarker() {
        return log4jMarker;
    }

    @Override
    public String getName() {
        return log4jMarker.getName();
    }

    @Override
    public boolean hasChildren() {
        return log4jMarker.hasParents();
    }

    @Override
    public int hashCode() {
        return 31 + Objects.hashCode(log4jMarker);
    }

    @Override
    public boolean hasReferences() {
        return log4jMarker.hasParents();
    }

    @Override
    public Iterator<Marker> iterator() {
        var log4jParents = log4jMarker.getParents();
        List<Marker> parents = new ArrayList<>(log4jParents.length);
        for (var m : log4jParents) {
            parents.add(markerFactory.getMarker(m.getName()));
        }
        return parents.iterator();
    }

    @Override
    public boolean remove(Marker slf4jMarker) {
        return slf4jMarker != null && log4jMarker.remove(MarkerManager.getMarker(slf4jMarker.getName()));
    }
}
