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
package org.neo4j.server.queryapi.response.format;

import static org.neo4j.server.queryapi.response.format.View.elementId;
import static org.neo4j.server.queryapi.response.format.View.endNodeElementId;
import static org.neo4j.server.queryapi.response.format.View.labels;
import static org.neo4j.server.queryapi.response.format.View.startNodeElementId;
import static org.neo4j.server.queryapi.response.format.View.type;

import com.fasterxml.jackson.annotation.JsonIncludeProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.io.Serial;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.value.LossyCoercion;
import org.neo4j.driver.summary.InputPosition;
import org.neo4j.driver.summary.Notification;
import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.Type;
import org.neo4j.driver.types.TypeSystem;
import org.neo4j.values.storable.DurationValue;

/**
 * A module that supports:
 * <ul>
 *     <li>{@link View#PLAIN_JSON} A plain json representation the serializers neo4j results to standard json types.</li>
 *     <li>{@link View#TYPED_JSON} A typed json representation which is encoded with type information of the form
 *     {@code {"$type": "<string_type_value>", "_value": <value representation>}}.</li>
 * </ul>
 *
 */
public final class DefaultResponseModule extends SimpleModule {

    @Serial
    private static final long serialVersionUID = -6600328341718439212L;

    /**
     * Needed for mostly all value serializers.
     */
    private final TypeSystem typeSystem;

    private final View view;

    /**
     * New type system delegating to the drivers {@link TypeSystem}.
     *
     * @param typeSystem Retrieved from the driver
     */
    public DefaultResponseModule(TypeSystem typeSystem, View view) {

        this.typeSystem = typeSystem;
        this.view = view;
        this.addSerializer(Record.class, new RecordSerializer());
        this.addSerializer(Value.class, new ValueSerializer());
        this.addSerializer(SummaryCounters.class, new SummaryCountersSerializer());
        this.addSerializer(Plan.class, new QueryPlanSerializer());
        this.addSerializer(ProfiledPlan.class, new ProfiledPlanSerializer());

        this.setMixInAnnotation(Notification.class, NotificationMixIn.class);
        this.setMixInAnnotation(InputPosition.class, InputPositionMixIn.class);
        this.setMixInAnnotation(Neo4jException.class, Neo4jExceptionMixIn.class);
    }

    /**
     * Not to be implemented. It is public only to be registered with GraalVM AoT via RegisterReflectionForBinding.
     */
    public interface NotificationMixIn {

        @JsonProperty
        String code();

        @JsonProperty
        String severity();

        @JsonProperty
        String title();

        @JsonProperty
        String description();

        @JsonProperty
        InputPosition position();
    }

    /**
     * Not to be implemented. It is public only to be registered with GraalVM AoT via RegisterReflectionForBinding.
     */
    public interface InputPositionMixIn {

        @JsonProperty
        int column();

        @JsonProperty
        int line();

        @JsonProperty
        int offset();
    }

    /**
     * Not to be implemented. It is public only to be registered with GraalVM AoT via RegisterReflectionForBinding.
     */
    @JsonIncludeProperties({"code", "message"})
    public interface Neo4jExceptionMixIn {

        @JsonProperty
        String code();
    }

    private static class SummaryCountersSerializer extends StdSerializer<SummaryCounters> {

        @Serial
        private static final long serialVersionUID = -4434233555324168878L;

        SummaryCountersSerializer() {
            super(SummaryCounters.class);
        }

        @Override
        public void serialize(SummaryCounters value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            if (value == null) {
                return;
            }
            gen.writeStartObject();
            gen.writeBooleanField("containsUpdates", value.containsUpdates());
            gen.writeNumberField("nodesCreated", value.nodesCreated());
            gen.writeNumberField("nodesDeleted", value.nodesDeleted());
            gen.writeNumberField("propertiesSet", value.propertiesSet());
            gen.writeNumberField("relationshipsCreated", value.relationshipsCreated());
            gen.writeNumberField("relationshipsDeleted", value.relationshipsDeleted());
            gen.writeNumberField("labelsAdded", value.labelsAdded());
            gen.writeNumberField("labelsRemoved", value.labelsRemoved());
            gen.writeNumberField("indexesAdded", value.indexesAdded());
            gen.writeNumberField("indexesRemoved", value.indexesRemoved());
            gen.writeNumberField("constraintsAdded", value.constraintsAdded());
            gen.writeNumberField("constraintsRemoved", value.constraintsRemoved());
            gen.writeBooleanField("containsSystemUpdates", value.containsSystemUpdates());
            gen.writeNumberField("systemUpdates", value.systemUpdates());
            gen.writeEndObject();
        }
    }

    private static class QueryPlanSerializer extends StdSerializer<Plan> {
        @Serial
        private static final long serialVersionUID = -6613007221451028541L;

        public QueryPlanSerializer() {
            super(Plan.class);
        }

        @Override
        public void serialize(Plan value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeStringField("operatorType", value.operatorType());
            gen.writeObjectField("arguments", value.arguments());
            gen.writeObjectField("identifiers", value.identifiers());
            gen.writeObjectField("children", value.children());
            gen.writeEndObject();
        }
    }

    private static final class ProfiledPlanSerializer extends StdSerializer<ProfiledPlan> {

        @Serial
        private static final long serialVersionUID = 8533863218949288878L;

        public ProfiledPlanSerializer() {
            super(ProfiledPlan.class);
        }

        @Override
        public void serialize(ProfiledPlan value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeStartObject();
            gen.writeNumberField("dbHits", value.dbHits());
            gen.writeNumberField("records", value.records());
            gen.writeBooleanField("hasPageCacheStats", value.hasPageCacheStats());
            gen.writeNumberField("pageCacheHits", value.pageCacheHits());
            gen.writeNumberField("pageCacheMisses", value.pageCacheMisses());
            gen.writeNumberField("pageCacheHitRatio", value.pageCacheHitRatio());
            gen.writeNumberField("time", value.time());
            gen.writeStringField("operatorType", value.operatorType());
            gen.writeObjectField("arguments", value.arguments());
            gen.writeObjectField("identifiers", value.identifiers());
            gen.writeObjectField("children", value.children());
            gen.writeEndObject();
        }
    }

    private static final class RecordSerializer extends StdSerializer<Record> {

        @Serial
        private static final long serialVersionUID = 8594507829627684699L;

        RecordSerializer() {
            super(Record.class);
        }

        @Override
        public void serialize(Record record, JsonGenerator json, SerializerProvider serializerProvider)
                throws IOException {

            var valueSerializer = serializerProvider.findValueSerializer(Value.class);
            for (Value value : record.values()) {
                valueSerializer.serialize(value, json, serializerProvider);
            }
        }
    }

    final class ValueSerializer extends StdSerializer<Value> {

        @Serial
        private static final long serialVersionUID = -5914605165093400044L;

        private static final Map<Integer, String> SRID_MAPPING = Map.of(
                7203, "cartesian",
                9157, "cartesian-3d",
                4326, "wgs-84",
                4979, "wgs-84-3d");
        private static final Map<Integer, String> FORMAT_MAPPING = Map.of(
                7203, "http://spatialreference.org/ref/sr-org/%d/ogcwkt/",
                9157, "http://spatialreference.org/ref/sr-org/%d/ogcwkt/",
                4326, "http://spatialreference.org/ref/epsg/%d/ogcwkt/",
                4979, "http://spatialreference.org/ref/epsg/%d/ogcwkt/");

        private final Map<Type, CypherTypes> typeToNames;

        ValueSerializer() {
            super(Value.class);
            this.typeToNames = new HashMap<>();
            typeToNames.put(typeSystem.BYTES(), CypherTypes.Base64);
            typeToNames.put(typeSystem.BOOLEAN(), CypherTypes.Boolean);
            typeToNames.put(typeSystem.INTEGER(), CypherTypes.Integer);
            typeToNames.put(typeSystem.NULL(), CypherTypes.Null);
            typeToNames.put(typeSystem.FLOAT(), CypherTypes.Float);
            typeToNames.put(typeSystem.STRING(), CypherTypes.String);
            typeToNames.put(typeSystem.DATE(), CypherTypes.Date);
            typeToNames.put(typeSystem.TIME(), CypherTypes.Time);
            typeToNames.put(typeSystem.LOCAL_TIME(), CypherTypes.LocalTime);
            typeToNames.put(typeSystem.DATE_TIME(), CypherTypes.DateTime);
            typeToNames.put(typeSystem.LOCAL_DATE_TIME(), CypherTypes.LocalDateTime);
            typeToNames.put(typeSystem.DURATION(), CypherTypes.Duration);
        }

        @Override
        public void serialize(Value value, JsonGenerator json, SerializerProvider serializers) throws IOException {

            if (value.hasType(typeSystem.LIST())) {
                if (view.equals(View.TYPED_JSON)) {
                    json.writeStartObject();
                    json.writeStringField(Fieldnames.CYPHER_TYPE, CypherTypes.List.getValue());
                    json.writeFieldName(Fieldnames.CYPHER_VALUE);
                    json.writeStartArray();
                } else {
                    json.writeStartArray();
                }

                for (Value element : value.values()) {
                    serialize(element, json, serializers);
                }
                json.writeEndArray();

                if (view.equals(View.TYPED_JSON)) {
                    json.writeEndObject();
                }
            } else if (value.hasType(typeSystem.MAP())
                    && !(value.hasType(typeSystem.NODE()) || value.hasType(typeSystem.RELATIONSHIP()))) {

                if (view.equals(View.TYPED_JSON)) {
                    json.writeStartObject();
                    json.writeStringField(Fieldnames.CYPHER_TYPE, CypherTypes.Map.getValue());
                    json.writeFieldName(Fieldnames.CYPHER_VALUE);
                }
                json.writeStartObject();
                for (String key : value.keys()) {
                    json.writeFieldName(key);
                    serialize(value.get(key), json, serializers);
                }
                json.writeEndObject();
                if (view.equals(View.TYPED_JSON)) {
                    json.writeEndObject();
                }
            } else if (view == View.PLAIN_JSON) {
                renderSimpleValue(value, json, serializers);
            } else if (view == View.TYPED_JSON) {
                renderNewFormat(value, json, serializers);
            }
        }

        private void renderNewFormat(Value value, JsonGenerator json, SerializerProvider serializers)
                throws IOException {

            if (typeToNames.containsKey(value.type())) {
                var cypherType = typeToNames.get(value.type());
                json.writeStartObject();
                if (value.hasType(typeSystem.DATE_TIME())) {
                    if (value.asZonedDateTime().getZone().normalized() instanceof ZoneOffset) {
                        json.writeStringField(Fieldnames.CYPHER_TYPE, "OffsetDateTime");
                    } else {
                        json.writeStringField(Fieldnames.CYPHER_TYPE, "ZonedDateTime");
                    }
                    json.writeStringField(
                            Fieldnames.CYPHER_VALUE,
                            typeToNames.get(value.type()).getWriter().apply(value));
                } else {
                    json.writeStringField(Fieldnames.CYPHER_TYPE, cypherType.getValue());
                    json.writeFieldName(Fieldnames.CYPHER_VALUE);
                    if (cypherType.equals(CypherTypes.Null)) { // use json types?
                        json.writeNull();
                    } else if (cypherType.equals(CypherTypes.Boolean)) {
                        json.writeBoolean(value.asBoolean());
                    } else {
                        json.writeString(cypherType.getWriter().apply(value));
                    }
                }
                json.writeEndObject();
            } else if (value.hasType(typeSystem.POINT())) {
                renderPoint(value, json, true);
            } else if (value.hasType(typeSystem.NODE())) {
                writeNode(value.asNode(), json, serializers, view);
            } else if (value.hasType(typeSystem.RELATIONSHIP())) {
                writeRelationship(value.asRelationship(), json, serializers, view);
            } else if (value.hasType(typeSystem.PATH())) {
                json.writeStartObject();
                json.writeStringField(Fieldnames.CYPHER_TYPE, CypherTypes.Path.getValue());
                json.writeFieldName(Fieldnames.CYPHER_VALUE);
                json.writeStartArray();
                var path = value.asPath();
                for (Path.Segment element : path) {
                    writeNode(element.start(), json, serializers, view);
                    writeRelationship(element.relationship(), json, serializers, view);
                }
                writeNode(path.end(), json, serializers, view);
                json.writeEndArray();
                json.writeEndObject();

            } else {
                throw new UnsupportedOperationException(
                        "Type " + value.type().name() + " is not supported as a column value");
            }
        }

        private void renderPoint(Value value, JsonGenerator json, boolean newFormat) throws IOException {

            var point = value.asPoint();
            json.writeStartObject();
            json.writeStringField(newFormat ? Fieldnames.CYPHER_TYPE : "type", "Point");
            if (newFormat) {
                json.writeFieldName(Fieldnames.CYPHER_VALUE);
                json.writeStartObject();
            }

            json.writeArrayFieldStart("coordinates");
            json.writeNumber(point.x());
            json.writeNumber(point.y());
            if (!Double.isNaN(point.z())) {
                json.writeNumber(point.z());
            }
            json.writeEndArray();
            json.writeObjectFieldStart("crs");
            json.writeNumberField("srid", point.srid());
            json.writeStringField("name", SRID_MAPPING.getOrDefault(point.srid(), "n/a"));
            json.writeStringField("type", "link");
            if (FORMAT_MAPPING.containsKey(point.srid())) {
                json.writeObjectFieldStart("properties");
                json.writeStringField("href", FORMAT_MAPPING.get(point.srid()).formatted(point.srid()));
                json.writeStringField("type", "ogcwkt");
                json.writeEndObject();
            }
            json.writeEndObject();

            if (newFormat) {
                json.writeEndObject();
            }
            json.writeEndObject();
        }

        private void renderSimpleValue(Value value, JsonGenerator json, SerializerProvider serializers)
                throws IOException {
            if (value == null || value.isNull()) {
                json.writeNull();
            } else if (value.hasType(typeSystem.BOOLEAN())) {
                json.writeBoolean(value.asBoolean());
            } else if (value.hasType(typeSystem.STRING())) {
                json.writeString(value.asString());
            } else if (value.hasType(typeSystem.INTEGER())) {
                json.writeNumber(value.asLong());
            } else if (value.hasType(typeSystem.FLOAT())) {
                try {
                    json.writeNumber(value.asFloat());
                } catch (LossyCoercion e) {
                    json.writeNumber(value.asDouble());
                }
            } else if (value.hasType(typeSystem.DATE())) {
                json.writeString(value.asLocalDate().format(DateTimeFormatter.ISO_DATE));
            } else if (value.hasType(typeSystem.DATE_TIME())) {
                json.writeString(value.asZonedDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
            } else if (value.hasType(typeSystem.DURATION())) {
                json.writeString(
                        DurationValue.parse(value.asIsoDuration().toString()).toString()); // todo 🤮
            } else if (value.hasType(typeSystem.LOCAL_DATE_TIME())) {
                json.writeString(value.asLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            } else if (value.hasType(typeSystem.LOCAL_TIME())) {
                json.writeString(value.asLocalTime().format(DateTimeFormatter.ISO_LOCAL_TIME));
            } else if (value.hasType(typeSystem.TIME())) {
                json.writeString(value.asOffsetTime().format(DateTimeFormatter.ISO_OFFSET_TIME));
            } else if (value.hasType(typeSystem.NODE())) {
                var node = value.asNode();
                writeNode(node, json, serializers, View.PLAIN_JSON);
            } else if (value.hasType(typeSystem.BYTES())) {
                json.writeString(Base64.getEncoder().encodeToString(value.asByteArray()));
            } else if (value.hasType(typeSystem.RELATIONSHIP())) {
                var rel = value.asRelationship();
                writeRelationship(rel, json, serializers, View.PLAIN_JSON);
            } else if (value.hasType(typeSystem.PATH())) {
                json.writeStartArray();
                var path = value.asPath();
                for (Path.Segment element : path) {
                    writeNode(element.start(), json, serializers, view);
                    writeRelationship(element.relationship(), json, serializers, View.PLAIN_JSON);
                }
                writeNode(path.end(), json, serializers, View.PLAIN_JSON);
                json.writeEndArray();
            } else if (value.hasType(typeSystem.POINT())) {
                renderPoint(value, json, false);
            } else {
                throw new UnsupportedOperationException(
                        "Type " + value.type().name() + " is not supported as a column value");
            }
        }

        private void writeEntityProperties(
                String propLabel, Entity node, JsonGenerator json, SerializerProvider serializers) throws IOException {
            json.writeFieldName(propLabel);
            json.writeStartObject();
            for (String property : node.keys()) {
                json.writeFieldName(property);
                serialize(node.get(property), json, serializers);
            }
            json.writeEndObject();
        }

        private void writeNode(Node node, JsonGenerator json, SerializerProvider serializers, View view)
                throws IOException {

            json.writeStartObject();

            if (view.equals(View.TYPED_JSON)) {
                json.writeStringField(Fieldnames.CYPHER_TYPE, CypherTypes.Node.name());
                json.writeFieldName(Fieldnames.CYPHER_VALUE);
                json.writeStartObject();
            }

            json.writeStringField(elementId(view), node.elementId());

            json.writeArrayFieldStart(labels(view));
            for (String s : node.labels()) {
                json.writeString(s);
            }
            json.writeEndArray();

            writeEntityProperties(View.properties(view), node, json, serializers);

            json.writeEndObject();

            if (view.equals(View.TYPED_JSON)) {
                json.writeEndObject();
            }
        }

        private void writeRelationship(
                Relationship relationship, JsonGenerator json, SerializerProvider serializers, View view)
                throws IOException {

            json.writeStartObject();

            if (view.equals(View.TYPED_JSON)) {
                json.writeStringField(Fieldnames.CYPHER_TYPE, CypherTypes.Relationship.name());
                json.writeFieldName(Fieldnames.CYPHER_VALUE);
                json.writeStartObject();
            }

            json.writeStringField(elementId(view), relationship.elementId());
            json.writeStringField(startNodeElementId(view), relationship.startNodeElementId());
            json.writeStringField(endNodeElementId(view), relationship.endNodeElementId());
            json.writeStringField(type(view), relationship.type());

            writeEntityProperties(View.properties(view), relationship, json, serializers);
            json.writeEndObject();

            if (view.equals(View.TYPED_JSON)) {
                json.writeEndObject();
            }
        }
    }
}
