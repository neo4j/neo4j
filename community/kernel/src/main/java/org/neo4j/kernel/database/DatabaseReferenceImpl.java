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
package org.neo4j.kernel.database;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.neo4j.configuration.helpers.RemoteUri;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;

/**
 *  Concrete implementations of this class represent different kinds of Database reference.
 *
 * - {@link Internal} references point to databases which are present in this DBMS.
 * - {@link External} references point to databases which are not present in this DBMS.
 *
 * A database may have multiple references, each with a different alias.
 * The reference whose {@link #alias()} corresponds to the database's original name is known as the primary reference.
 */
public abstract class DatabaseReferenceImpl implements DatabaseReference {
    private static final Comparator<DatabaseReference> referenceComparator =
            Comparator.comparing(a -> a.alias().name(), String::compareToIgnoreCase);
    private static final Comparator<DatabaseReference> nullSafeReferenceComparator =
            Comparator.nullsLast(referenceComparator);
    private static final NormalizedDatabaseName defaultNamespace =
            new NormalizedDatabaseName(TopologyGraphDbmsModel.DEFAULT_NAMESPACE);

    @Override
    public int compareTo(DatabaseReference that) {
        return nullSafeReferenceComparator.compare(this, that);
    }

    @Override
    public String toPrettyString() {
        var namespace = namespace().map(ns -> ns.name() + ".").orElse("");
        var name = alias().name();
        return namespace + name;
    }

    @Override
    public NormalizedDatabaseName fullName() {
        var namespace = namespace().map(ns -> ns.name() + ".").orElse("");
        var name = alias().name();
        return new NormalizedDatabaseName(namespace + name);
    }

    @Override
    public boolean isComposite() {
        return false;
    }

    /**
     * External references point to databases which are not stored within this DBMS.
     */
    public static final class External extends DatabaseReferenceImpl {
        private final NormalizedDatabaseName targetAlias;
        private final NormalizedDatabaseName alias;
        private final NormalizedDatabaseName namespace;
        private final RemoteUri externalUri;
        private final UUID uuid;

        /**
         * Creates an external database reference with no namespace (default namespace)
         */
        public External(
                NormalizedDatabaseName targetAlias, NormalizedDatabaseName alias, RemoteUri externalUri, UUID uuid) {
            this(targetAlias, alias, null, externalUri, uuid);
        }

        /**
         * Creates an external database reference
         */
        public External(
                NormalizedDatabaseName targetAlias,
                NormalizedDatabaseName alias,
                NormalizedDatabaseName namespace,
                RemoteUri externalUri,
                UUID uuid) {
            this.targetAlias = targetAlias;
            this.alias = alias;
            this.namespace = Objects.equals(namespace, defaultNamespace) ? null : namespace;
            this.externalUri = externalUri;
            this.uuid = uuid;
        }

        @Override
        public NormalizedDatabaseName alias() {
            return alias;
        }

        @Override
        public Optional<NormalizedDatabaseName> namespace() {
            return Optional.ofNullable(namespace);
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
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            External external = (External) o;
            return Objects.equals(targetAlias, external.targetAlias)
                    && Objects.equals(alias, external.alias)
                    && Objects.equals(namespace, external.namespace)
                    && Objects.equals(externalUri, external.externalUri)
                    && Objects.equals(uuid, external.uuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetAlias, alias, namespace, externalUri, uuid);
        }

        @Override
        public String toString() {
            return "External{"
                    + "alias=" + alias
                    + ", namespace=" + namespace
                    + ", remoteUri=" + externalUri
                    + ", remoteName=" + targetAlias
                    + ", uuid=" + uuid
                    + '}';
        }
    }

    /**
     * Local references point to databases which are stored within this DBMS.
     *
     * Note, however, that a local reference may point to databases not stored on this physical instance.
     */
    public static sealed class Internal extends DatabaseReferenceImpl {
        protected final NormalizedDatabaseName alias;
        protected final NormalizedDatabaseName namespace;
        protected final NamedDatabaseId namedDatabaseId;
        protected final boolean primary;

        /**
         * Creates an internal database reference with no namespace (default namespace)
         */
        public Internal(NormalizedDatabaseName alias, NamedDatabaseId namedDatabaseId, boolean primary) {
            this(alias, null, namedDatabaseId, primary);
        }

        /**
         * Creates an internal database reference
         */
        public Internal(
                NormalizedDatabaseName alias,
                NormalizedDatabaseName namespace,
                NamedDatabaseId namedDatabaseId,
                boolean primary) {
            this.alias = alias;
            this.namespace = Objects.equals(namespace, defaultNamespace) ? null : namespace;
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
        public Optional<NormalizedDatabaseName> namespace() {
            return Optional.ofNullable(namespace);
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
                    && Objects.equals(namespace, internal.namespace)
                    && Objects.equals(namedDatabaseId, internal.namedDatabaseId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, namespace, namedDatabaseId, primary);
        }

        @Override
        public String toString() {
            return "Internal{" + "alias="
                    + alias + ", namespace="
                    + namespace + ", namedDatabaseId="
                    + namedDatabaseId + ", primary="
                    + primary + '}';
        }
    }

    public static final class Composite extends DatabaseReferenceImpl.Internal {
        private final List<DatabaseReference> constituents;

        /**
         * Creates a composite database reference
         */
        public Composite(
                NormalizedDatabaseName alias, NamedDatabaseId namedDatabaseId, Set<DatabaseReference> constituents) {
            super(alias, namedDatabaseId, true);
            this.constituents = constituents.stream().sorted().toList();
        }

        @Override
        public Optional<NormalizedDatabaseName> namespace() {
            return Optional.empty();
        }

        public List<DatabaseReference> constituents() {
            return constituents;
        }

        public Optional<DatabaseReference> getConstituentByName(String databaseName) {
            for (DatabaseReference constituent : constituents) {
                if (constituent.fullName().equals(new NormalizedDatabaseName(databaseName))) {
                    return Optional.of(constituent);
                }
            }
            return Optional.empty();
        }

        public Optional<DatabaseReference> getConstituentById(UUID databaseId) {
            for (DatabaseReference constituent : constituents) {
                if (constituent.id().equals(databaseId)) {
                    return Optional.of(constituent);
                }
            }
            return Optional.empty();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Composite composite = (Composite) o;
            return Objects.equals(constituents, composite.constituents);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), constituents);
        }

        @Override
        public String toString() {
            return "Composite{" + "alias="
                    + alias + ", namespace="
                    + namespace + ", namedDatabaseId="
                    + namedDatabaseId + ", primary="
                    + primary + ", constituents="
                    + constituents + '}';
        }

        @Override
        public boolean isComposite() {
            return true;
        }
    }

    public static final class SPD extends DatabaseReferenceImpl.Internal {
        public static String shardName(String databaseName, int index) {
            return String.format("%s-shard-%02d", databaseName, index);
        }

        public static <T> Map<Integer, T> createForShards(
                String databaseName, int count, Function<String, Optional<T>> mapper) {
            return IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> i, i -> mapper.apply(
                            shardName(databaseName, i))
                    .orElseThrow()));
        }

        private final Map<Integer, DatabaseReference> entityDetailStores;

        /**
         * Creates a sharded property database reference
         */
        public SPD(
                NormalizedDatabaseName alias,
                NamedDatabaseId namedDatabaseId,
                Map<Integer, DatabaseReference> entityDetailStores) {
            super(alias, namedDatabaseId, true);
            this.entityDetailStores = entityDetailStores;
        }

        @Override
        public Optional<NormalizedDatabaseName> namespace() {
            return Optional.empty();
        }

        public Map<Integer, DatabaseReference> entityDetailStores() {
            return entityDetailStores;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            SPD spd = (SPD) o;
            return Objects.equals(entityDetailStores, spd.entityDetailStores());
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), entityDetailStores);
        }

        @Override
        public String toString() {
            return "ShardedPropertyDatabase{" + "alias="
                    + alias + ", namespace="
                    + namespace + ", namedDatabaseId="
                    + namedDatabaseId + ", primary="
                    + primary + ", entityDetailStores="
                    + entityDetailStores + '}';
        }
    }
}
