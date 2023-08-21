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
package org.neo4j.kernel.api.impl.schema.vector;

import static org.apache.lucene.document.Field.Store.NO;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.neo4j.values.storable.FloatingPointArray;

class VectorDocumentStructure {
    static final String ENTITY_ID_KEY = "id";
    static final String VECTOR_VALUE_KEY = "vector";

    static Term newTermForChangeOrRemove(long id) {
        return new Term(ENTITY_ID_KEY, Long.toString(id));
    }

    static Document createLuceneDocument(
            long id, FloatingPointArray value, VectorSimilarityFunction similarityFunction) {
        final var vector = similarityFunction.maybeToValidVector(value);
        if (vector == null) {
            return null;
        }

        final var document = new Document();
        final var idField = new StringField(ENTITY_ID_KEY, Long.toString(id), NO);
        final var idValueField = new NumericDocValuesField(ENTITY_ID_KEY, id);
        document.add(idField);
        document.add(idValueField);
        final var fieldType = new VectorFieldType(vector.length, similarityFunction);
        final var valueField = new KnnFloatVectorField(VECTOR_VALUE_KEY, vector, fieldType);
        document.add(valueField);
        return document;
    }

    /** Lucene's {@link FieldType#setVectorAttributes} enforces a max dimensionality,
     * but otherwise just sets {@link FieldType#vectorDimension}, {@link FieldType#vectorSimilarityFunction}, and
     * {@link FieldType#vectorEncoding}.
     * <p>
     * We can just extend {@link FieldType} with our own implementation that supplies
     * those values without any such max dimensionality check to circumvent that.
     */
    private static class VectorFieldType extends FieldType {
        private final int vectorDimension;
        private final VectorSimilarityFunction similarityFunction;

        private VectorFieldType(int dimension, VectorSimilarityFunction similarityFunction) {
            this.vectorDimension = dimension;
            this.similarityFunction = similarityFunction;
            freeze();
        }

        @Override
        public int vectorDimension() {
            return vectorDimension;
        }

        @Override
        public org.apache.lucene.index.VectorSimilarityFunction vectorSimilarityFunction() {
            return similarityFunction.toLucene();
        }

        @Override
        public VectorEncoding vectorEncoding() {
            return VectorEncoding.FLOAT32;
        }
    }
}
