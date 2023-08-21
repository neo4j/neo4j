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
package org.neo4j.kernel.api.impl.schema.trigram;

import static org.apache.lucene.document.Field.Store.NO;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

class TrigramDocumentStructure {
    static final String ENTITY_ID_KEY = "id";
    static final String TRIGRAM_VALUE_KEY = "0";

    static Term newTermForChangeOrRemove(long nodeId) {
        return new Term(ENTITY_ID_KEY, "" + nodeId);
    }

    static Document createLuceneDocument(long id, Value value) {
        var document = new Document();
        var idField = new StringField(ENTITY_ID_KEY, Long.toString(id), NO);
        var idValueField = new NumericDocValuesField(ENTITY_ID_KEY, id);
        document.add(idField);
        document.add(idValueField);
        if (value.valueGroup() == ValueGroup.TEXT) {
            var tokenStream = new TrigramTokenStream(value.asObject().toString());
            var valueField = new TrigramField(TRIGRAM_VALUE_KEY, tokenStream);
            document.add(valueField);
        }

        return document;
    }

    private static class TrigramField extends Field {
        private static final FieldType TYPE = new FieldType();

        static {
            TYPE.setOmitNorms(true);
            TYPE.setIndexOptions(IndexOptions.DOCS);
            TYPE.setTokenized(true);
            TYPE.setStored(false);
            TYPE.freeze();
        }

        public TrigramField(String name, TokenStream tokenStream) {
            super(name, tokenStream, TYPE);
        }
    }
}
