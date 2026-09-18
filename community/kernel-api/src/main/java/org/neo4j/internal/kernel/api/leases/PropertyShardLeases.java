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
package org.neo4j.internal.kernel.api.leases;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.Leases;

public class PropertyShardLeases implements Leases {
    public static final PropertyShardLeases EMPTY = new PropertyShardLeases(Map.of());
    // Use a SortedMap to ensure deterministic iteration and serialization
    private final SortedMap<DatabaseId, PropertyShardLease> leases;

    public PropertyShardLeases(Map<DatabaseId, PropertyShardLease> leases) {
        this.leases = new TreeMap<>(Comparator.comparing(DatabaseId::uuid));
        this.leases.putAll(leases);
    }

    public static PropertyShardLeases from(Map<DatabaseId, PropertyShardLease> leases) {
        if (leases.isEmpty()) return EMPTY;
        return new PropertyShardLeases(leases);
    }

    @Override
    public int size() {
        return leases.size();
    }

    @Override
    public PropertyShardLease get(DatabaseId databaseId) {
        return leases.get(databaseId);
    }

    @Override
    public Iterator<Lease> iterator() {
        return leases.values().stream().map(l -> (Lease) l).iterator();
    }

    public Optional<PropertyShardLease> getLease(DatabaseId propertyShardId) {
        return Optional.ofNullable(get(propertyShardId));
    }

    public PropertyShardLeases updateLease(DatabaseId propertyShardId, ServerId serverId) {
        PropertyShardLease lease = leases.get(propertyShardId);
        long leaseId = 0;
        if (lease != null) {
            leaseId = lease.id() + 1;
        }
        return PropertyShardLeases.from(
                Map.of(propertyShardId, new PropertyShardLease(propertyShardId, serverId, leaseId)));
    }

    public PropertyShardLeases merge(PropertyShardLeases nextLeases) {
        SortedMap<DatabaseId, PropertyShardLease> copy = new TreeMap<>(this.leases);
        if (isValidTransition(nextLeases)) {
            copy.putAll(nextLeases.leases);
        }
        return new PropertyShardLeases(copy);
    }

    public boolean doLeasesMatch(Leases other) {
        for (Lease otherLease : other) {
            DatabaseId propertyShardId = otherLease.databaseId();
            if (!leases.containsKey(propertyShardId)) {
                return false;
            }
            PropertyShardLease currentLease = leases.get(propertyShardId);

            if (!currentLease.equals(PropertyShardLease.from(otherLease))) {
                return false;
            }
        }
        return true;
    }

    private boolean isValidTransition(PropertyShardLeases nextLeases) {
        for (Lease next : nextLeases) {
            PropertyShardLease currentLease = leases.get(next.databaseId());
            if (currentLease != null && !currentLease.isValidTransition(next)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "PropertyShardLeases{leases=" + leases + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PropertyShardLeases other)) return false;
        return Objects.equals(leases, other.leases);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(leases);
    }

    public record PropertyShardLease(DatabaseId databaseId, ServerId serverId, long id) implements Lease {
        boolean isValidTransition(Lease nextLease) {
            return nextLease.id() > this.id;
        }

        public static PropertyShardLease from(Lease lease) {
            return new PropertyShardLease(lease.databaseId(), lease.serverId(), lease.id());
        }
    }
}
