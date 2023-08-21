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
package org.neo4j.kernel.api.impl.schema;

import static org.apache.lucene.document.Field.Store.YES;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class TextDocumentStructure {

    public static final String NODE_ID_KEY = "id";

    private static final ThreadLocal<DocWithId> perThreadDocument = ThreadLocal.withInitial(DocWithId::new);
    public static final String DELIMITER = "\u001F";

    private TextDocumentStructure() {}

    private static DocWithId reuseDocument(long nodeId) {
        DocWithId doc = perThreadDocument.get();
        doc.setId(nodeId);
        return doc;
    }

    public static Document documentRepresentingProperties(long nodeId, Value... values) {
        DocWithId document = reuseDocument(nodeId);
        document.setValues(values);
        return document.document;
    }

    public static MatchAllDocsQuery newScanQuery() {
        return new MatchAllDocsQuery();
    }

    public static Query newSeekQuery(Value... values) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (int i = 0; i < values.length; i++) {
            builder.add(ValueEncoding.String.encodeQuery(values[i], i), BooleanClause.Occur.MUST);
        }
        return builder.build();
    }

    public static Term newTermForChangeOrRemove(long nodeId) {
        return new Term(NODE_ID_KEY, "" + nodeId);
    }

    public static long getNodeId(Document from) {
        return Long.parseLong(from.get(NODE_ID_KEY));
    }

    public static boolean useFieldForUniquenessVerification(String fieldName) {
        return !TextDocumentStructure.NODE_ID_KEY.equals(fieldName)
                && ValueEncoding.fieldPropertyNumber(fieldName) == 0;
    }

    private static class DocWithId {
        private final Document document;

        private final Field idField;
        private final Field idValueField;

        private Field[] reusableValueFields = new Field[0];

        private DocWithId() {
            idField = new StringField(NODE_ID_KEY, "", YES);
            idValueField = new NumericDocValuesField(NODE_ID_KEY, 0L);
            document = new Document();
            document.add(idField);
            document.add(idValueField);
        }

        private void setId(long id) {
            idField.setStringValue(Long.toString(id));
            idValueField.setLongValue(id);
        }

        private void setValues(Value... values) {
            removeAllValueFields();
            int neededLength = values.length * ValueEncoding.values().length;
            if (reusableValueFields.length < neededLength) {
                reusableValueFields = new Field[neededLength];
            }

            for (int i = 0; i < values.length; i++) {
                if (values[i].valueGroup() == ValueGroup.TEXT) {
                    Field reusableField = getFieldWithValue(i, values[i]);
                    document.add(reusableField);
                }
            }
        }

        private void removeAllValueFields() {
            document.clear();
            document.add(idField);
            document.add(idValueField);
        }

        private Field getFieldWithValue(int propertyNumber, Value value) {
            int reuseId = propertyNumber * ValueEncoding.values().length + ValueEncoding.String.ordinal();
            String key = ValueEncoding.String.key(propertyNumber);
            Field reusableField = reusableValueFields[reuseId];
            if (reusableField == null) {
                reusableField = ValueEncoding.String.encodeField(key, value);
                reusableValueFields[reuseId] = reusableField;
            } else {
                ValueEncoding.String.setFieldValue(value, reusableField);
            }
            return reusableField;
        }
    }
}
