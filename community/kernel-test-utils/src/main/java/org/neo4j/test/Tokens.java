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
package org.neo4j.test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.neo4j.exceptions.KernelException;
import org.neo4j.kernel.api.KernelTransaction;

/**
 * Utility classes for unifying concepts around tokens for testing.
 */
public class Tokens {
    /**
     * A set of factory helpers that normalize the creation of tokens.
     */
    public static class Factories {
        public static final Label LABEL = new Label();
        public static final RelationshipType RELATIONSHIP_TYPE = new RelationshipType();
        public static final PropertyKey PROPERTY_KEY = new PropertyKey();

        /**
         * A factory helper to normalize the creation of {@link org.neo4j.graphdb.Label}s.
         */
        public static final class Label extends Factory<org.neo4j.graphdb.Label> {
            @Override
            public org.neo4j.graphdb.Label fromName(String name) {
                return org.neo4j.graphdb.Label.label(name);
            }

            @Override
            public int getId(KernelTransaction tx, org.neo4j.graphdb.Label label) throws KernelException {
                return tx.tokenWrite().labelGetOrCreateForName(label.name());
            }

            private Label() {}
        }

        /**
         * A factory helper to normalize the creation of {@link org.neo4j.graphdb.RelationshipType}s.
         */
        public static final class RelationshipType extends Factory<org.neo4j.graphdb.RelationshipType> {

            @Override
            public org.neo4j.graphdb.RelationshipType fromName(String name) {
                return org.neo4j.graphdb.RelationshipType.withName(name);
            }

            @Override
            public int getId(KernelTransaction tx, org.neo4j.graphdb.RelationshipType relType) throws KernelException {
                return tx.tokenWrite().relationshipTypeGetOrCreateForName(relType.name());
            }

            private RelationshipType() {}
        }

        /**
         * A factory helper to normalize the creation of property keys.
         */
        public static final class PropertyKey extends Factory<String> {

            @Override
            public String fromName(String name) {
                return name;
            }

            @Override
            public int getId(KernelTransaction tx, String propertyKey) throws KernelException {
                return tx.tokenWrite().propertyKeyGetOrCreateForName(propertyKey);
            }

            private PropertyKey() {}
        }
        /**
         * An abstract factory helper to normalize the creation of {@code TOKEN}s.
         *
         * @param <TOKEN> the type of tokens to be created
         */
        public abstract static sealed class Factory<TOKEN> permits Label, RelationshipType, PropertyKey {
            /**
             * @param name the name of a {@code TOKEN}
             * @return a {@code TOKEN} corresponding to {@code name}
             */
            public abstract TOKEN fromName(String name);

            /**
             * Get the ID for the given {@code token}.
             * <p>
             * If the {@code token} has a corresponding ID, fetch that ID; otherwise, create and write token, returning its ID.
             *
             * @param tx  the transaction in which to acquire the ID
             * @param token the {@code TOKEN} who's ID is to be acquired
             * @return the ID of the corresponding {@code TOKEN} token
             * @throws KernelException if the corresponding ID of the given {@code token} cannot be acquired
             */
            public abstract int getId(KernelTransaction tx, TOKEN token) throws KernelException;

            /**
             * Get the IDs for the given {@code token}.
             *
             * @param tx   the transaction in which to acquire the ID
             * @param tokens the {@code TOKEN}s who's IDs are to be acquired
             * @return the IDs of the corresponding {@code TOKEN} tokens
             * @throws KernelException if the corresponding ID of the given {@code tokens} cannot be acquired
             * @see Factory#getId(KernelTransaction, Object)
             */
            @SafeVarargs
            public final int[] getIds(KernelTransaction tx, TOKEN... tokens) throws KernelException {
                final var ids = new int[tokens.length];
                for (int i = 0; i < tokens.length; i++) {
                    ids[i] = getId(tx, tokens[i]);
                }
                return ids;
            }

            /**
             * Get the IDs for the given {@code tokens}.
             *
             * @param tx   the transaction in which to acquire the ID
             * @param tokens the {@code TOKEN}s who's IDs are to be acquired
             * @return the IDs of the corresponding {@code TOKEN} tokens
             * @throws KernelException if the corresponding ID of the given {@code tokens} cannot be acquired
             * @see Factory#getId(KernelTransaction, Object)
             */
            public final int[] getIds(KernelTransaction tx, Iterable<TOKEN> tokens) throws KernelException {
                final var ids = IntLists.mutable.empty();
                for (final var token : tokens) {
                    ids.add(getId(tx, token));
                }
                return ids.toArray();
            }
        }

        private Factories() {}
    }

    /**
     * A set of suppliers that can autogenerate tokens.
     */
    public static class Suppliers {
        /**
         * A namespace for a {@code static} set of {@link Supplier}s using {@link Suffixes#incrementing()}.
         * <p>
         * Note that the underlying value for the incrementing suffix will be shared across threads, and the lifetime of the program; for each factory
         * respectively. If you wish to have tokens named locally to your scope/thread/test/etc; it would be best to construct your own.
         * <pre>{@code
         * final var global = Tags.Suppliers.Incrementing.LABEL;
         * final var local  = new Tags.Suppliers.Label(Tags.Suppliers.Suffixes.incrementing());
         * }</pre>
         */
        public static class Incrementing {
            public static final Label LABEL = new Label(Suffixes.incrementing());
            public static final RelationshipType RELATIONSHIP_TYPE = new RelationshipType(Suffixes.incrementing());
            public static final PropertyKey PROPERTY_KEY = new PropertyKey(Suffixes.incrementing());

            private Incrementing() {}
        }

        /**
         * A namespace for a {@code static} set of {@link Supplier}s using {@link Suffixes#UUID()}.
         */
        public static class UUID {
            public static final Label LABEL = new Label(Suffixes.UUID());
            public static final RelationshipType RELATIONSHIP_TYPE = new RelationshipType(Suffixes.UUID());
            public static final PropertyKey PROPERTY_KEY = new PropertyKey(Suffixes.UUID());

            private UUID() {}
        }

        /**
         * A set of methods to help create common suffix {@link java.util.function.Supplier}s for {@link Supplier}s.
         */
        public static class Suffixes {
            /**
             * Incrementing suffix
             *
             * <pre>{@code
             * final var label = new Tags.Suppliers.Label(Tags.Suppliers.Suffixes.incrementing());
             * label.get(5);  // [ Label_0, Label_1, Label_2, Label_3, Label_4 ]
             * }</pre>
             *
             * @return a thread safe suffix {@link java.util.function.Supplier} of a {@code long} which increments from {@code 0}
             * @see Suffixes#incrementing(long)
             */
            public static java.util.function.Supplier<String> incrementing() {
                return incrementing(0);
            }

            /**
             * Incrementing suffix, from a value
             *
             * <pre>{@code
             * final var label = new Tags.Suppliers.Label(Tags.Suppliers.Suffixes.incrementing( 42 ));
             * label.get(5);  // [ Label_42, Label_43, Label_44, Label_45, Label_46 ]
             * }</pre>
             *
             * @param from the value from which to increment
             * @return a thread safe suffix {@link java.util.function.Supplier} of a {@code long} which increments from {@code from}
             */
            public static java.util.function.Supplier<String> incrementing(long from) {
                final var count = new AtomicLong(from);
                return () -> String.valueOf(count.getAndIncrement());
            }

            /**
             * Random UUID suffix
             *
             * <pre>{@code
             * final var label = new Tags.Suppliers.Label(Tags.Suppliers.Suffixes.UUID());
             * label.get(5);  // [ Label_407412ea-1585-46a6-8f4b-bfe4e2e04c7a,
             *                //   Label_53c1f5f6-947a-4222-93b0-7572901db0dc,
             *                //   Label_456c9989-3db6-4a8a-89ea-976b8d8b1289,
             *                //   Label_59ba5fd6-13cc-4343-acd0-f2b145ac70d1,
             *                //   Label_6ea86439-8031-4964-8d26-c97e1b89caf9 ]
             * }</pre>
             *
             * @return a suffix {@link java.util.function.Supplier} of a random UUID
             * @see java.util.UUID#randomUUID()
             */
            public static java.util.function.Supplier<String> UUID() {
                return () -> java.util.UUID.randomUUID().toString();
            }

            /**
             * Random hexadecimal suffix
             *
             * <pre>{@code
             * final var random = new Random(0xdeadbeef);
             * final var label = new Tags.Suppliers.Label(Tags.Suppliers.Suffixes.random(random, 4));
             * label.get(5);  // [ Label_2f25353b, Label_30d5c9d2, Label_59f9b1e4, Label_4b5ca001, Label_12b3c84a ]
             * }</pre>
             *
             * @param random   a source of randomness
             * @param numBytes number of bytes of randomness to use
             * @return a suffix {@link java.util.function.Supplier} of a random number of bytes in represented in hexadecimal
             */
            public static java.util.function.Supplier<String> random(Random random, int numBytes) {
                return () -> {
                    final var bytes = new byte[numBytes];
                    random.nextBytes(bytes);
                    final var sb = new StringBuilder();
                    for (final var b : bytes) {
                        sb.append(String.format("%02x", b));
                    }
                    return sb.toString();
                };
            }

            private Suffixes() {}
        }

        /**
         * A {@link java.util.function.Supplier} that autogenerates {@link org.neo4j.graphdb.Label}s.
         */
        public static final class Label extends Supplier<org.neo4j.graphdb.Label> {
            /**
             * @param name      the name prefix of the autogenerated {@link org.neo4j.graphdb.Label}s
             * @param separator the separator between the {@code name} and the supplied {@code suffix}
             * @param suffix    a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated {@link org.neo4j.graphdb.Label}s
             */
            public Label(String name, String separator, java.util.function.Supplier<String> suffix) {
                super(name, separator, suffix, Factories.LABEL);
            }

            /**
             * @param name   the name of the autogenerated {@link org.neo4j.graphdb.Label}s
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated {@link org.neo4j.graphdb.Label}s
             * @see Supplier#Supplier(String, java.util.function.Supplier, Factories.Factory)
             */
            public Label(String name, java.util.function.Supplier<String> suffix) {
                super(name, suffix, Factories.LABEL);
            }

            /**
             * Constructs with a default name of {@code "Label"}.
             *
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated {@link org.neo4j.graphdb.Label}s
             * @see Label#Label(String, java.util.function.Supplier)
             */
            public Label(java.util.function.Supplier<String> suffix) {
                this("Label", suffix);
            }
        }

        /**
         * A {@link java.util.function.Supplier} that autogenerates {@link org.neo4j.graphdb.RelationshipType}s.
         */
        public static final class RelationshipType extends Supplier<org.neo4j.graphdb.RelationshipType> {
            /**
             * @param name      the name prefix of the autogenerated {@link org.neo4j.graphdb.RelationshipType}s
             * @param separator the separator between the {@code name} and the supplied {@code suffix}
             * @param suffix    a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated
             *                  {@link org.neo4j.graphdb.RelationshipType}s
             */
            public RelationshipType(String name, String separator, java.util.function.Supplier<String> suffix) {
                super(name, separator, suffix, Factories.RELATIONSHIP_TYPE);
            }

            /**
             * @param name   the name of the autogenerated {@link org.neo4j.graphdb.RelationshipType}s
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated
             *               {@link org.neo4j.graphdb.RelationshipType}s
             * @see Supplier#Supplier(String, java.util.function.Supplier, Factories.Factory)
             */
            public RelationshipType(String name, java.util.function.Supplier<String> suffix) {
                super(name, suffix, Factories.RELATIONSHIP_TYPE);
            }

            /**
             * Constructs with default name of {@code "RelationshipType"}.
             *
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated
             *               {@link org.neo4j.graphdb.RelationshipType}s
             * @see RelationshipType#RelationshipType(String, java.util.function.Supplier)
             */
            public RelationshipType(java.util.function.Supplier<String> suffix) {
                this("RelationshipType", suffix);
            }
        }

        /**
         * A {@link java.util.function.Supplier} that autogenerates property keys.
         */
        public static final class PropertyKey extends Supplier<String> {
            /**
             * @param name      the name prefix of the autogenerated property keys
             * @param separator the separator between the {@code name} and the supplied {@code suffix}
             * @param suffix    a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated property keys
             */
            public PropertyKey(String name, String separator, java.util.function.Supplier<String> suffix) {
                super(name, separator, suffix, Factories.PROPERTY_KEY);
            }

            /**
             * @param name   the name of the autogenerated property keys
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated property keys
             * @see Supplier#Supplier(String, java.util.function.Supplier, Factories.Factory)
             */
            public PropertyKey(String name, java.util.function.Supplier<String> suffix) {
                super(name, suffix, Factories.PROPERTY_KEY);
            }

            /**
             * Constructs with a default name of {@code "PropertyKey"}.
             *
             * @param suffix a {@link java.util.function.Supplier} of suffixes to append to the name for autogenerated property keys
             * @see PropertyKey#PropertyKey(String, java.util.function.Supplier)
             */
            public PropertyKey(java.util.function.Supplier<String> suffix) {
                this("PropertyKey", suffix);
            }
        }

        /**
         * An abstract {@link java.util.function.Supplier} that autogenerates {@code TOKEN}s.
         *
         * @param <TOKEN> the type of tokens to be supplied
         */
        public abstract static sealed class Supplier<TOKEN> implements java.util.function.Supplier<TOKEN>
                permits Label, RelationshipType, PropertyKey {
            private final String name;
            private final String separator;
            private final java.util.function.Supplier<String> suffix;
            private final Factories.Factory<TOKEN> factory;

            /**
             * @param name      the name of the token
             * @param separator the separator between the {@code name} and the supplied {@code suffix}
             * @param suffix    a {@link java.util.function.Supplier} of suffixes to append to the name of autogenerated {@code TOKEN}s
             * @param factory
             */
            protected Supplier(
                    String name,
                    String separator,
                    java.util.function.Supplier<String> suffix,
                    Factories.Factory<TOKEN> factory) {
                this.name = name;
                this.separator = separator;
                this.suffix = suffix;
                this.factory = factory;
            }

            /**
             * Constructs with a default separator of {@code "_"}.
             *
             * @param name    the name of the token
             * @param suffix  a {@link java.util.function.Supplier} of suffixes to append to the name of autogenerated {@code TOKEN}s
             * @param factory
             * @see Supplier#Supplier(String, String, java.util.function.Supplier, Factories.Factory)
             */
            protected Supplier(
                    String name, java.util.function.Supplier<String> suffix, Factories.Factory<TOKEN> factory) {
                this(name, "_", suffix, factory);
            }

            /**
             * The name of the {@link Supplier} class to be used as a prefix to autogenerated {@code TOKEN}s, and for debugging purposes.
             *
             * @return the name of the {@link Supplier} class
             */
            public final String name() {
                return name;
            }

            /**
             * Supply a {@code TOKEN}.
             *
             * @return an autogenerated {@code TOKEN}
             * @implSpec <p>behaves as if generated from <pre>{@code factory.fromName(name + separator + suffix.get())}</pre>
             */
            @Override
            public final TOKEN get() {
                return factory.fromName(name + separator + suffix.get());
            }

            /**
             * Supply a given number of {@code TOKEN}s.
             *
             * @param numberOfTags the number of {@code TOKEN}s to generate
             * @return autogenerated {@code TOKEN}s
             * @see Supplier#get()
             */
            public final List<TOKEN> get(int numberOfTags) {
                return Stream.generate(this).limit(numberOfTags).toList();
            }

            /**
             * Get the corresponding ID of an autogenerated {@code TOKEN}.
             *
             * @param tx the transaction in which to acquire the ID
             * @return the ID of the corresponding {@code TOKEN} token as if from {@link Supplier#get()}
             * @throws KernelException if the corresponding ID of the generated {@code TOKEN} cannot be acquired
             * @see Supplier#getId(KernelTransaction)
             */
            public final int getId(KernelTransaction tx) throws KernelException {
                return factory.getId(tx, get());
            }

            /**
             * Get the IDs for a given number of autogenerated {@code TOKEN}s.
             *
             * @param tx           the transaction in which to acquire the IDs
             * @param numberOfTags the number of {@code TOKEN}s to generate
             * @return the IDs of the corresponding autogenerated {@code TOKEN}s
             * @throws KernelException if any of the corresponding IDs of the generated {@code TOKEN}s cannot be acquired
             */
            public final int[] getIds(KernelTransaction tx, int numberOfTags) throws KernelException {
                return factory.getIds(tx, get(numberOfTags));
            }
        }

        private Suppliers() {}
    }

    private Tokens() {}
}
