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
package org.neo4j.kernel.api.impl.index.lucene.v10;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.NANOS;
import static java.time.temporal.ChronoUnit.SECONDS;

import java.util.function.Consumer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocument;
import org.neo4j.kernel.api.impl.index.lucene.LuceneDocumentsFactory;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.BooleanField;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.SingleDoubleField;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.SingleInstantField;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.SingleIntegerField;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.SingleLongField;
import org.neo4j.kernel.api.impl.index.lucene.v10.Lucene10ValueFields.TemporalWithZone;
import org.neo4j.kernel.api.impl.schema.vector.Neo4jVectorSimilarityFunction;
import org.neo4j.kernel.api.impl.schema.vector.VectorDocumentStructure;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

public class Lucene10DocumentsFactory implements LuceneDocumentsFactory {
    public static final LuceneDocumentsFactory INSTANCE = new Lucene10DocumentsFactory();

    @Override
    public LuceneDocument newDocument() {
        return new Lucene10Document();
    }

    @Override
    public LuceneDocument reusableTextDocument(long id, Value... values) {
        return Lucene10ReusableDocWithId.reusableTextDocument(id, values);
    }

    @Override
    public LuceneDocument reusableFulltextDocument(long id, String[] propertyNames, Value[] values) {
        return Lucene10ReusableDocWithId.reusableFulltextDocument(id, propertyNames, values);
    }

    @Override
    public LuceneDocument createTrigramDocument(long id, Value value) {
        Lucene10Document document = new Lucene10Document();
        document.addStringField(ENTITY_ID_KEY, Long.toString(id), false);
        document.addNumericDocValuesField(ENTITY_ID_KEY, id);
        if (value.valueGroup() == ValueGroup.TEXT) {
            Lucene10TrigramTokenStream tokenStream =
                    new Lucene10TrigramTokenStream(value.asObject().toString());
            TrigramField valueField = new TrigramField(TRIGRAM_VALUE_KEY, tokenStream);
            document.document.add(valueField);
        }

        return document;
    }

    @Override
    public LuceneDocument createVectorDocument(
            VectorDocumentStructure vectorDocumentStructure,
            long id,
            Neo4jVectorSimilarityFunction similarityFunction,
            Value[] values) {
        Preconditions.checkArgument(
                values != null && values.length >= 1,
                "%s vector document has no values",
                getClass().getSimpleName());
        float[] vector = LuceneDocumentsFactory.maybeVectorFromValues(values, similarityFunction);
        if (vector == null) {
            return null;
        }

        Lucene10Document document = new Lucene10Document();
        document.addStringField(ENTITY_ID_KEY, Long.toString(id), false);
        document.addNumericDocValuesField(ENTITY_ID_KEY, id);
        document.addKnnFloatVectorField(
                vectorDocumentStructure.vectorValueKeyFor(vector.length), vector, similarityFunction);

        for (int i = 1; i < values.length; i++) {
            Value value = values[i];
            if (value == null) {
                continue;
            }
            document.addStringField(EXISTS_KEY, new BytesRef(Lucene10ValueFields.intToBytes(i)), false);
            addIndexableFields(vectorDocumentStructure, i, value, document.document::add);
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

        private TrigramField(String name, TokenStream tokenStream) {
            super(name, tokenStream, TYPE);
        }
    }

    /**
     * Create the Lucene field(s) which represent the value in the vector index
     * "add" these fields using the supplied consumer.
     * <p>
     * Most types of value straightforwardly are represented with a single field (numeric or text values)
     * Some types of value (e.g. temporal) result in the creation of multiple fields.
     * <p>
     * Receiving a null or not being able to index able specific value, is not a failure.
     * If a value cannot be indexed, it is simply skipped;
     * this is how it other type-restrictive indexes are handled.
     *
     * @param vectorDocumentStructure tells us how to name fields
     * @param index tells us the index of the property which the value represents
     * @param value the value to be written to the field - guaranteed not to be null here
     * @param addField is called with every created field, and may directly add the field to a document,
     * or perform some kind of test action.
     */
    static void addIndexableFields(
            VectorDocumentStructure vectorDocumentStructure,
            int index,
            Value value,
            Consumer<IndexableField> addField) {
        switch (value) {
            case BooleanValue bv ->
                addField.accept(new BooleanField(vectorDocumentStructure.booleanValueKeyFor(index), bv.booleanValue()));
            case IntegralValue iv ->
                addField.accept(
                        new SingleLongField(vectorDocumentStructure.integralValueKeyFor(index), iv.longValue()));
            case FloatingPointValue fv ->
                addField.accept(
                        new SingleDoubleField(vectorDocumentStructure.floatingValueKeyFor(index), fv.doubleValue()));
            case TextValue tv ->
                addField.accept(
                        new StringField(vectorDocumentStructure.textValueKeyFor(index), tv.stringValue(), Store.NO));
            case TemporalValue<?, ?> tv -> {
                TemporalWithZone<?, ?> twz = Lucene10ValueFields.storedFromTemporal(tv);
                addField.accept(new SingleInstantField(
                        vectorDocumentStructure.temporalValueKeyFor(index, tv.valueGroup()), twz.instant()));
                if (twz.hasZoneOffset()) {
                    addField.accept(new SingleIntegerField(
                            vectorDocumentStructure.zoneOffsetValueKeyFor(index, tv.valueGroup()),
                            twz.zoneOffset().getTotalSeconds()));
                    if (twz.hasZoneId()) {
                        addField.accept(new StringField(
                                vectorDocumentStructure.zoneIdValueKeyFor(index, tv.valueGroup()),
                                TemporalWithZone.zoneIdString(twz.zoneId()),
                                Store.NO));
                    }
                }
            }
            case DurationValue d -> {
                long nanos = d.get(NANOS);
                long seconds = d.get(SECONDS);
                long days = d.get(DAYS);
                long months = d.get(MONTHS);
                addField.accept(new SingleLongField(vectorDocumentStructure.durationMonthsValueKeyFor(index), months));
                addField.accept(new SingleLongField(vectorDocumentStructure.durationDaysValueKeyFor(index), days));
                addField.accept(
                        new SingleLongField(vectorDocumentStructure.durationSecondsValueKeyFor(index), seconds));
                addField.accept(new SingleLongField(vectorDocumentStructure.durationNanosValueKeyFor(index), nanos));
            }
            case null ->
                throw InvalidArgumentException.invalidFunctionArgument(
                        "addIndexableFields", "Invalid input for 'addIndexableFields': the value parameter is null");
            default -> {}
        }
    }
}
