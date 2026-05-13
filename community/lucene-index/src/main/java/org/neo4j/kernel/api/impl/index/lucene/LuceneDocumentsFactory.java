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
package org.neo4j.kernel.api.impl.index.lucene;

import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10DocumentsFactory;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.values.VectorCandidate;
import org.neo4j.values.storable.Value;

public interface LuceneDocumentsFactory {
    LuceneDocumentsFactory CURRENT = Lucene10DocumentsFactory.INSTANCE;

    String ENTITY_ID_KEY = "id";
    String TEXT_VALUE_KEY_SUFFIX = "string";

    String TRIGRAM_VALUE_KEY = "0";
    String EXISTS_KEY = "exists";

    /**
     * Get the lucene document field name for a text value.
     * @param propertyNumber The property index, in case of multiple properties
     * @return the field name used text indexes.
     */
    static String textValueKey(int propertyNumber) {
        if (propertyNumber == 0) {
            return TEXT_VALUE_KEY_SUFFIX;
        }
        return propertyNumber + TEXT_VALUE_KEY_SUFFIX;
    }

    static boolean isFirstTextProperty(String fieldName) {
        return !Character.isDigit(fieldName.charAt(0));
    }

    /**
     * Get the node or relationship id from the document.
     *
     * @param document document to extract id from.
     * @return the node or relationship id.
     */
    static long getEntityId(LuceneDocument document) {
        return Long.parseLong(document.get(LuceneDocumentsFactory.ENTITY_ID_KEY));
    }

    /**
     * Helper to extract a valid vector from the vector document values parameter
     * @param values values configuring the vector document. the first value should be the vector itself
     * @param similarityFunction checks the validity of the vector in the context of the function
     * @return a valid {@code float[]} representation of the vector, otherwise {@code null}.
     */
    static float[] maybeVectorFromValues(Value[] values, Neo4jVectorSimilarityFunction similarityFunction) {
        VectorCandidate candidate = VectorCandidate.maybeFrom(values[0]);
        if (candidate == null) {
            return null;
        }
        return similarityFunction.maybeToValidVector(candidate);
    }

    LuceneDocument reusableTextDocument(long id, Value... values);

    LuceneDocument reusableFulltextDocument(long id, String[] propertyNames, Value[] values);

    LuceneDocument createTrigramDocument(long id, Value value);

    LuceneDocument createVectorDocument(
            VectorDocumentStructure vectorDocumentStructure,
            long id,
            Neo4jVectorSimilarityFunction similarityFunction,
            Value[] values);

    LuceneDocument newDocument();
}
