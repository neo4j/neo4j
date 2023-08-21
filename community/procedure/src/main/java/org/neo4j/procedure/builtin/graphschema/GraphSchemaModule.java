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
package org.neo4j.procedure.builtin.graphschema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.io.Serial;
import java.util.Collection;
import java.util.List;

/**
 * Encapsulating all the Jackson-JSON stuff.
 */
final class GraphSchemaModule extends SimpleModule {

    @Serial
    private static final long serialVersionUID = -4886300307467434436L;

    private static volatile ObjectMapper OBJECT_MAPPER;

    static ObjectMapper getGraphSchemaObjectMapper() {
        var result = OBJECT_MAPPER;
        if (result == null) {
            synchronized (GraphSchemaModule.class) {
                result = OBJECT_MAPPER;
                if (result == null) {
                    OBJECT_MAPPER = new ObjectMapper();
                    OBJECT_MAPPER.registerModule(new GraphSchemaModule());
                    result = OBJECT_MAPPER;
                }
            }
        }
        return result;
    }

    // The nested maps render quite useless in browser
    static String asJsonString(List<GraphSchema.Property> properties) {
        try {
            return getGraphSchemaObjectMapper().writeValueAsString(properties);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private GraphSchemaModule() {
        addSerializer(GraphSchema.Type.class, new TypeSerializer());
        addSerializer(GraphSchema.class, new GraphSchemaSerializer());
        addSerializer(GraphSchema.Ref.class, new RefSerializer());
        setMixInAnnotation(GraphSchema.Property.class, PropertyMixin.class);
        setMixInAnnotation(GraphSchema.NodeObjectType.class, NodeObjectTypeMixin.class);
        setMixInAnnotation(GraphSchema.Token.class, TokenMixin.class);
        setMixInAnnotation(GraphSchema.RelationshipObjectType.class, RelationshipObjectTypeMixin.class);
    }

    private static final class GraphSchemaSerializer extends StdSerializer<GraphSchema> {

        @Serial
        private static final long serialVersionUID = 3421593591346480162L;

        GraphSchemaSerializer() {
            super(GraphSchema.class);
        }

        @Override
        public void serialize(GraphSchema value, JsonGenerator gen, SerializerProvider provider) throws IOException {

            gen.writeStartObject();
            gen.writeObjectFieldStart("graphSchemaRepresentation");
            gen.writeFieldName("graphSchema");
            gen.writeStartObject();
            writeArray(gen, "nodeLabels", value.nodeLabels().values());
            writeArray(gen, "relationshipTypes", value.relationshipTypes().values());
            writeArray(gen, "nodeObjectTypes", value.nodeObjectTypes().values());
            writeArray(
                    gen,
                    "relationshipObjectTypes",
                    value.relationshipObjectTypes().values());
            gen.writeEndObject();
            gen.writeEndObject();
            gen.writeEndObject();
        }

        private void writeArray(JsonGenerator gen, String fieldName, Collection<?> items) throws IOException {
            gen.writeArrayFieldStart(fieldName);
            for (Object item : items) {
                gen.writeObject(item);
            }
            gen.writeEndArray();
        }
    }

    private static final class TypeSerializer extends StdSerializer<GraphSchema.Type> {

        @Serial
        private static final long serialVersionUID = -1260953273076427362L;

        TypeSerializer() {
            super(GraphSchema.Type.class);
        }

        @Override
        public void serialize(GraphSchema.Type type, JsonGenerator gen, SerializerProvider serializerProvider)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("type", type.value());
            if (type.value().equals("array")) {
                gen.writeObjectFieldStart("items");
                gen.writeStringField("type", type.itemType());
                gen.writeEndObject();
            }
            gen.writeEndObject();
        }
    }

    @JsonPropertyOrder({"token", "type", "nullable"})
    private abstract static class PropertyMixin {

        @JsonProperty("type")
        @JsonSerialize(using = TypeListSerializer.class)
        abstract List<GraphSchema.Type> types();

        @JsonProperty("nullable")
        @JsonSerialize(using = InvertingBooleanSerializer.class)
        abstract boolean mandatory();
    }

    private static class InvertingBooleanSerializer extends StdSerializer<Boolean> {

        @Serial
        private static final long serialVersionUID = 6272997898442893145L;

        InvertingBooleanSerializer() {
            super(Boolean.class);
        }

        @Override
        public void serialize(Boolean value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            gen.writeBoolean(!Boolean.TRUE.equals(value));
        }
    }

    @JsonPropertyOrder({"id", "labels", "properties"})
    private abstract static class NodeObjectTypeMixin {

        @JsonProperty("$id")
        abstract String id();
    }

    @JsonPropertyOrder({"id", "value"})
    private abstract static class TokenMixin {

        @JsonProperty("$id")
        abstract String id();

        @JsonProperty("token")
        abstract String value();
    }

    private static final class RefSerializer extends StdSerializer<GraphSchema.Ref> {

        @Serial
        private static final long serialVersionUID = -3928051476420574836L;

        RefSerializer() {
            super(GraphSchema.Ref.class);
        }

        @Override
        public void serialize(GraphSchema.Ref value, JsonGenerator gen, SerializerProvider provider)
                throws IOException {
            gen.writeStartObject();
            gen.writeObjectField("$ref", "#" + value.value());
            gen.writeEndObject();
        }
    }

    @JsonPropertyOrder({"id", "type", "from", "to", "properties"})
    private abstract static class RelationshipObjectTypeMixin {

        @JsonProperty("$id")
        abstract String id();
    }

    private static final class TypeListSerializer extends StdSerializer<List<GraphSchema.Type>> {

        @Serial
        private static final long serialVersionUID = -8831424337461613203L;

        TypeListSerializer() {
            super(TypeFactory.defaultInstance().constructType(new TypeReference<List<GraphSchema.Type>>() {}));
        }

        @Override
        public void serialize(List<GraphSchema.Type> types, JsonGenerator gen, SerializerProvider serializerProvider)
                throws IOException {
            if (types.isEmpty()) {
                gen.writeNull();
            } else if (types.size() == 1) {
                gen.writeObject(types.get(0));
            } else {
                gen.writeObject(types);
            }
        }
    }
}
