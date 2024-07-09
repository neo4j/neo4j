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
package org.neo4j.internal.batchimport.input;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.internal.batchimport.input.csv.Decorator;

/**
 * Common {@link InputEntityVisitor} decorators, able to provide defaults or overrides.
 */
public class InputEntityDecorators {
    private InputEntityDecorators() {}

    /**
     * Ensures that all input nodes will at least have the given set of labels.
     */
    public static Decorator additiveLabels(final String... labelNamesToAdd) {
        if (labelNamesToAdd == null || labelNamesToAdd.length == 0) {
            return NO_DECORATOR;
        }

        return node -> new AdditiveLabelsDecorator(node, labelNamesToAdd);
    }

    /**
     * Ensures that input relationships without a specified relationship type will get
     * the specified default relationship type.
     */
    public static Decorator defaultRelationshipType(final String defaultType) {
        return defaultType == null
                ? NO_DECORATOR
                : relationship -> new RelationshipTypeDecorator(relationship, defaultType);
    }

    private static final class AdditiveLabelsDecorator extends InputEntityVisitor.Delegate {
        private final String[] transport = new String[1];
        private final String[] labelNamesToAdd;
        private final boolean[] seenLabels;
        private boolean seenLabelField;

        AdditiveLabelsDecorator(InputEntityVisitor actual, String[] labelNamesToAdd) {
            super(actual);
            this.labelNamesToAdd = labelNamesToAdd;
            this.seenLabels = new boolean[labelNamesToAdd.length];
        }

        @Override
        public boolean labelField(long labelField) {
            seenLabelField = true;
            return super.labelField(labelField);
        }

        @Override
        public boolean labels(String[] labels) {
            if (!seenLabelField) {
                for (String label : labels) {
                    for (int i = 0; i < labelNamesToAdd.length; i++) {
                        if (!seenLabels[i] && labelNamesToAdd[i].equals(label)) {
                            seenLabels[i] = true;
                        }
                    }
                }
            }
            return super.labels(labels);
        }

        @Override
        public void endOfEntity() throws IOException {
            if (!seenLabelField) {
                for (int i = 0; i < seenLabels.length; i++) {
                    if (!seenLabels[i]) {
                        transport[0] = labelNamesToAdd[i];
                        super.labels(transport);
                    }
                }
            }

            Arrays.fill(seenLabels, false);
            seenLabelField = false;
            super.endOfEntity();
        }
    }

    private static final class RelationshipTypeDecorator extends InputEntityVisitor.Delegate {
        private final String defaultType;
        private boolean hasType;

        RelationshipTypeDecorator(InputEntityVisitor actual, String defaultType) {
            super(actual);
            this.defaultType = defaultType;
        }

        @Override
        public boolean type(int type) {
            hasType = true;
            return super.type(type);
        }

        @Override
        public boolean type(String type) {
            if (type != null) {
                hasType = true;
            }
            return super.type(type);
        }

        @Override
        public void endOfEntity() throws IOException {
            if (!hasType) {
                super.type(defaultType);
                hasType = false;
            }

            super.endOfEntity();
        }
    }

    public static Decorator decorators(final Decorator... decorators) {
        return new Decorator() {
            @Override
            public InputEntityVisitor apply(InputEntityVisitor from) {
                for (Decorator decorator : decorators) {
                    from = decorator.apply(from);
                }
                return from;
            }

            @Override
            public boolean isMutable() {
                return Stream.of(decorators).anyMatch(Decorator::isMutable);
            }
        };
    }

    public static final Decorator NO_DECORATOR = value -> value;
}
