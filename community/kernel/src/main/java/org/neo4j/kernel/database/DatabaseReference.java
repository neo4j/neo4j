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
package org.neo4j.kernel.database;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.neo4j.configuration.helpers.RemoteUri;

/**
 * Implementations of this interface represent different kinds of Database reference.
 *
 * - {@link Internal} references point to databases which are present in this DBMS.
 * - {@link External} references point to databases which are not present in this DBMS.
 *
 * A database may have multiple references, each with a different alias.
 * The reference whose {@link #alias()} corresponds to the database's original name is known as the primary reference.
 */
public abstract class DatabaseReference implements Comparable<DatabaseReference> {
    private static final Comparator<DatabaseReference> referenceComparator =
            Comparator.comparing(a -> a.alias().name(), String::compareToIgnoreCase);
    private static final Comparator<DatabaseReference> nullSafeReferenceComparator =
            Comparator.nullsLast(referenceComparator);

    /**
     * @return the alias associated with this database reference
     */
    public abstract NormalizedDatabaseName alias();

    /**
     * @return whether the alias associated with this reference is the database's original/true name
     */
    public abstract boolean isPrimary();

    /**
     * @return the unique identity for this reference
     */
    public abstract UUID id();

    @Override
    public int compareTo(DatabaseReference that) {
        return nullSafeReferenceComparator.compare(this, that);
    }

    /**
     * External references point to databases which are not stored within this DBMS.
     */
    public static final class External extends DatabaseReference {
        private final NormalizedDatabaseName targetAlias;
        private final NormalizedDatabaseName alias;
        private final RemoteUri externalUri;
        private final UUID uuid;

        public External(
                NormalizedDatabaseName targetAlias, NormalizedDatabaseName alias, RemoteUri externalUri, UUID uuid) {
            this.targetAlias = targetAlias;
            this.alias = alias;
            this.externalUri = externalUri;
            this.uuid = uuid;
        }

        @Override
        public NormalizedDatabaseName alias() {
            return alias;
        }

        @Override
        public boolean isPrimary() {
            return false;
        }

        public RemoteUri externalUri() {
            return externalUri;
        }

        public NormalizedDatabaseName targetAlias() {
            return targetAlias;
        }

        @Override
        public UUID id() {
            return uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            External remote = (External) o;
            return Objects.equals(targetAlias, remote.targetAlias)
                    && Objects.equals(alias, remote.alias)
                    && Objects.equals(externalUri, remote.externalUri)
                    && Objects.equals(uuid, remote.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetAlias, alias, externalUri, uuid);
        }

        @Override
        public String toString() {
            return "DatabaseReference.External{" + "remoteName="
                    + targetAlias + ", name="
                    + alias + ", remoteUri="
                    + externalUri + ", uuid="
                    + uuid + '}';
        }
    }

    /**
     * Local references point to databases which are stored within this DBMS.
     *
     * Note, however, that a local reference may point to databases not stored on this physical instance.
     */
    public static final class Internal extends DatabaseReference {
        private final NormalizedDatabaseName alias;
        private final NamedDatabaseId namedDatabaseId;
        private final boolean primary;

        public Internal(NormalizedDatabaseName alias, NamedDatabaseId namedDatabaseId, boolean primary) {
            this.alias = alias;
            this.namedDatabaseId = namedDatabaseId;
            this.primary = primary;
        }

        public NamedDatabaseId databaseId() {
            return namedDatabaseId;
        }

        @Override
        public NormalizedDatabaseName alias() {
            return alias;
        }

        @Override
        public boolean isPrimary() {
            return primary;
        }

        @Override
        public UUID id() {
            return namedDatabaseId.databaseId().uuid();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Internal internal = (Internal) o;
            return primary == internal.primary
                    && Objects.equals(alias, internal.alias)
                    && Objects.equals(namedDatabaseId, internal.namedDatabaseId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, namedDatabaseId, primary);
        }

        @Override
        public String toString() {
            return "DatabaseReference.Internal{" + "alias="
                    + alias + ", namedDatabaseId="
                    + namedDatabaseId + ", primary="
                    + primary + '}';
        }
    }

    public static final class Composite extends DatabaseReference {
        private final NormalizedDatabaseName alias;
        private final NamedDatabaseId namedDatabaseId;
        private final List<DatabaseReference> components;

        public Composite(
                NormalizedDatabaseName alias, NamedDatabaseId namedDatabaseId, Set<DatabaseReference> components) {
            this.alias = alias;
            this.namedDatabaseId = namedDatabaseId;
            this.components = components.stream().sorted().toList();
        }

        public NamedDatabaseId databaseId() {
            return namedDatabaseId;
        }

        @Override
        public NormalizedDatabaseName alias() {
            return alias;
        }

        @Override
        public boolean isPrimary() {
            return Objects.equals(alias.name(), namedDatabaseId.name());
        }

        @Override
        public UUID id() {
            return namedDatabaseId.databaseId().uuid();
        }

        public List<DatabaseReference> components() {
            return components;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Composite composite = (Composite) o;
            return Objects.equals(alias, composite.alias)
                    && Objects.equals(namedDatabaseId, composite.namedDatabaseId)
                    && Objects.equals(components, composite.components);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, namedDatabaseId, components);
        }

        @Override
        public String toString() {
            return "DatabaseReference.Composite{" + "alias="
                    + alias + ", namedDatabaseId="
                    + namedDatabaseId + ", components="
                    + components + '}';
        }
    }
}
