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
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.status.StatusLogger;
import org.slf4j.IMarkerFactory;
import org.slf4j.Marker;

public class SLF4JToLog4jMarkerFactory implements IMarkerFactory {
    private static final Logger LOGGER = StatusLogger.getLogger();

    private final ConcurrentMap<String, Marker> markerMap = new ConcurrentHashMap<>();

    @Override
    public Marker getMarker(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Marker name must not be null");
        }
        Marker marker = markerMap.get(name);
        if (marker != null) {
            return marker;
        }
        org.apache.logging.log4j.Marker log4jMarker = MarkerManager.getMarker(name);
        return addMarkerIfAbsent(name, log4jMarker);
    }

    private Marker addMarkerIfAbsent(String name, org.apache.logging.log4j.Marker log4jMarker) {
        Marker marker = new SLF4JToLog4jMarker(this, log4jMarker);
        Marker existing = markerMap.putIfAbsent(name, marker);
        return existing == null ? marker : existing;
    }

    private Marker getMarker(Marker marker) {
        if (marker == null) {
            throw new IllegalArgumentException("Marker must not be null");
        }
        Marker m = markerMap.get(marker.getName());
        if (m != null) {
            return m;
        }
        return addMarkerIfAbsent(marker.getName(), convertMarker(marker));
    }

    org.apache.logging.log4j.Marker getLog4jMarker(Marker marker) {
        if (marker == null) {
            return null;
        } else if (marker instanceof SLF4JToLog4jMarker) {
            return ((SLF4JToLog4jMarker) marker).getLog4jMarker();
        } else {
            return ((SLF4JToLog4jMarker) getMarker(marker)).getLog4jMarker();
        }
    }

    private static org.apache.logging.log4j.Marker convertMarker(Marker original) {
        if (original == null) {
            throw new IllegalArgumentException("Marker must not be null");
        }
        return convertMarker(original, new ArrayList<>());
    }

    private static org.apache.logging.log4j.Marker convertMarker(Marker original, Collection<Marker> visited) {
        org.apache.logging.log4j.Marker marker = MarkerManager.getMarker(original.getName());
        if (original.hasReferences()) {
            Iterator<Marker> it = original.iterator();
            while (it.hasNext()) {
                Marker next = it.next();
                if (visited.contains(next)) {
                    LOGGER.warn("Found a cycle in Marker [{}]. Cycle will be broken.", next.getName());
                } else {
                    visited.add(next);
                    marker.addParents(convertMarker(next, visited));
                }
            }
        }
        return marker;
    }

    @Override
    public boolean exists(String name) {
        return markerMap.containsKey(name);
    }

    @Override
    public boolean detachMarker(String name) {
        return false;
    }

    @Override
    public Marker getDetachedMarker(String name) {
        LOGGER.warn("Log4j does not support detached Markers. Returned Marker [{}] will be unchanged.", name);
        return getMarker(name);
    }
}
