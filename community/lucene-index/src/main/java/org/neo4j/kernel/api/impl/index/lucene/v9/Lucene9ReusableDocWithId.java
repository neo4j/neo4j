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
package org.neo4j.kernel.api.impl.index.lucene.v9;

import static org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory.ENTITY_ID_KEY;
import static org.neo4j.kernel.api.impl.schema.fulltext.LuceneFulltextDocumentStructure.FIELD_ENTITY_ID;
import static org.neo4j.shaded.lucene9.document.Field.Store.NO;
import static org.neo4j.shaded.lucene9.document.Field.Store.YES;

import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.shaded.lucene9.document.Document;
import org.neo4j.shaded.lucene9.document.Field;
import org.neo4j.shaded.lucene9.document.NumericDocValuesField;
import org.neo4j.shaded.lucene9.document.StringField;
import org.neo4j.shaded.lucene9.document.TextField;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

abstract class Lucene9ReusableDocWithId {
    final LuceneDocument luceneDocument;
    final Document document;

    final Field idField;
    final Field idValueField;

    Lucene9ReusableDocWithId(String fieldEntityId) {
        idField = new StringField(fieldEntityId, "", YES);
        idValueField = new NumericDocValuesField(fieldEntityId, 0L);
        document = new Document();
        document.add(idField);
        document.add(idValueField);
        luceneDocument = new Lucene9Document(document);
    }

    static LuceneDocument reusableTextDocument(long nodeId, Value... values) {
        Lucene9ReusableTextDocWithId doc = Lucene9ReusableTextDocWithId.PER_THREAD_DOCUMENT.get();
        doc.setId(nodeId);
        doc.setValues(values);
        return doc.luceneDocument;
    }

    static LuceneDocument reusableFulltextDocument(long id, String[] propertyNames, Value[] values) {
        Lucene9ReusableFulltextDocWithId doc = Lucene9ReusableFulltextDocWithId.PER_THREAD_DOCUMENT.get();
        doc.setId(id);
        int setValues = doc.setValues(propertyNames, values);
        return setValues == 0 ? null : doc.luceneDocument;
    }

    void setId(long id) {
        resetFields();
        idField.setStringValue(Long.toString(id));
        idValueField.setLongValue(id);
    }

    private void resetFields() {
        document.clear();
        document.add(idField);
        document.add(idValueField);
    }

    private static class Lucene9ReusableTextDocWithId extends Lucene9ReusableDocWithId {

        private static final ThreadLocal<Lucene9ReusableTextDocWithId> PER_THREAD_DOCUMENT =
                ThreadLocal.withInitial(Lucene9ReusableTextDocWithId::new);
        private Field[] reusableValueFields = new Field[0];

        Lucene9ReusableTextDocWithId() {
            super(ENTITY_ID_KEY);
        }

        public void setValues(Value... values) {
            int neededLength = values.length;
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

        private Field getFieldWithValue(int propertyNumber, Value value) {
            String key = LuceneDocumentsFactory.textValueKey(propertyNumber);
            Field reusableField = reusableValueFields[propertyNumber];
            if (reusableField == null) {
                reusableField = new StringField(key, value.asObject().toString(), NO);
                reusableValueFields[propertyNumber] = reusableField;
            } else {
                reusableField.setStringValue(value.asObject().toString());
            }
            return reusableField;
        }
    }

    private static class Lucene9ReusableFulltextDocWithId extends Lucene9ReusableDocWithId {
        private static final ThreadLocal<Lucene9ReusableFulltextDocWithId> PER_THREAD_DOCUMENT =
                ThreadLocal.withInitial(Lucene9ReusableFulltextDocWithId::new);

        private Lucene9ReusableFulltextDocWithId() {
            super(FIELD_ENTITY_ID);
        }

        public int setValues(String[] names, Value[] values) {
            int i = 0;
            int nbrAddedValues = 0;
            for (String name : names) {
                Value value = values[i++];
                if (value != null) {
                    if (value.valueGroup() == ValueGroup.TEXT) {
                        document.add(encodeValueField(name, value));
                        nbrAddedValues++;
                    }
                    if (value.valueGroup() == ValueGroup.TEXT_ARRAY) {
                        TextArray array = (TextArray) value;
                        for (AnyValue val : array) {
                            document.add(encodeValueField(name, (Value) val));
                        }
                        nbrAddedValues++;
                    }
                }
            }
            return nbrAddedValues;
        }

        private static Field encodeValueField(String propertyKey, Value value) {
            TextValue textValue = (TextValue) value;
            String stringValue = textValue.stringValue();
            return new TextField(propertyKey, stringValue, NO);
        }
    }
}
