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
package org.neo4j.server.http.cypher.format.output.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import javax.ws.rs.core.MediaType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.server.http.cypher.format.api.RecordEvent;
import org.neo4j.server.rest.repr.ListWriter;
import org.neo4j.server.rest.repr.MappingWriter;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationBasedMessageBodyWriter;
import org.neo4j.server.rest.repr.RepresentationFormat;
import org.neo4j.server.rest.repr.formats.JsonFormat;

class RestRepresentationWriter implements ResultDataContentWriter {
    private final URI baseUri;

    RestRepresentationWriter(URI baseUri) {
        this.baseUri = baseUri;
    }

    @Override
    public void write(JsonGenerator out, RecordEvent recordEvent) throws IOException {
        WriteThroughJsonFormat format = new WriteThroughJsonFormat(out);
        out.writeArrayFieldStart("rest");
        try {
            for (String key : recordEvent.getColumns()) {
                write(out, format, recordEvent.getValue(key));
            }
        } finally {
            out.writeEndArray();
        }
    }

    /**
     * Recursively walks through the {@literal value}. We can't use {@literal ObjectToRepresentationConverter}
     * for converting everything into a {@link Representation}.
     *
     * @param out
     * @param format
     * @param value
     * @throws IOException
     */
    private void write(JsonGenerator out, WriteThroughJsonFormat format, Object value) throws IOException {
        if (value instanceof Map<?, ?>) {
            out.writeStartObject();
            try {
                for (Map.Entry<String, ?> entry : ((Map<String, ?>) value).entrySet()) {
                    out.writeFieldName(entry.getKey());
                    write(out, format, entry.getValue());
                }
            } finally {
                out.writeEndObject();
            }
        } else if (value instanceof Path) {
            RepresentationBasedMessageBodyWriter.serialize(new PathRepresentation<>((Path) value), format, baseUri);
        } else if (value instanceof Iterable<?>) {
            out.writeStartArray();
            try {
                for (Object item : (Iterable<?>) value) {
                    write(out, format, item);
                }
            } finally {
                out.writeEndArray();
            }
        } else if (value instanceof Node) {
            NodeRepresentation representation = new NodeRepresentation((Node) value);
            RepresentationBasedMessageBodyWriter.serialize(representation, format, baseUri);
        } else if (value instanceof Relationship) {
            RelationshipRepresentation representation = new RelationshipRepresentation((Relationship) value);
            RepresentationBasedMessageBodyWriter.serialize(representation, format, baseUri);
        } else {
            format.serializeValue(null, value);
        }
    }

    /**
     * In contrast to {@link JsonFormat}, this {@link RepresentationFormat} does write directly to a {@link JsonFactory} respectively {@link JsonGenerator}. It
     * does not create new factory nor codes on each write value (in contrast to {@literal JsonFormat} and it's usage of {@literal JsonHelper}.
     */
    private static class WriteThroughJsonFormat extends RepresentationFormat {
        private final JsonGenerator jsonGenerator;

        WriteThroughJsonFormat(JsonGenerator g) {
            super(MediaType.APPLICATION_JSON_TYPE);
            this.jsonGenerator = g;
        }

        @Override
        protected String serializeValue(String type, Object value) {
            try {
                jsonGenerator.writeObject(value);
                return null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        protected ListWriter serializeList(String type) {
            return new WriteThroughListWriterImpl(jsonGenerator);
        }

        @Override
        public MappingWriter serializeMapping(String type) {
            return new WriteThroughMappingWriterImpl(jsonGenerator);
        }

        @Override
        protected String complete(ListWriter serializer) {
            flush();
            return null;
        }

        @Override
        protected String complete(MappingWriter serializer) {
            flush();
            return null;
        }

        private void flush() {
            try {
                jsonGenerator.flush();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void complete() {
            flush();
        }
    }

    private static class WriteThroughMappingWriterImpl extends MappingWriter {
        private final JsonGenerator jsonGenerator;

        WriteThroughMappingWriterImpl(JsonGenerator jsonGenerator) {
            this.jsonGenerator = jsonGenerator;
            try {
                jsonGenerator.writeStartObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        WriteThroughMappingWriterImpl(JsonGenerator g, String key) {
            this.jsonGenerator = g;
            try {
                g.writeObjectFieldStart(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public MappingWriter newMapping(String type, String key) {
            return new WriteThroughMappingWriterImpl(jsonGenerator, key);
        }

        @Override
        public ListWriter newList(String type, String key) {
            return new WriteThroughListWriterImpl(jsonGenerator, key);
        }

        @Override
        public void writeValue(String type, String key, Object value) {
            try {
                jsonGenerator.writeObjectField(key, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void done() {
            try {
                jsonGenerator.writeEndObject();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class WriteThroughListWriterImpl extends ListWriter {
        private final JsonGenerator jsonGenerator;

        WriteThroughListWriterImpl(JsonGenerator jsonGenerator) {
            this.jsonGenerator = jsonGenerator;
            try {
                jsonGenerator.writeStartArray();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        WriteThroughListWriterImpl(JsonGenerator g, String key) {
            this.jsonGenerator = g;
            try {
                g.writeArrayFieldStart(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public MappingWriter newMapping(String type) {
            return new WriteThroughMappingWriterImpl(jsonGenerator);
        }

        @Override
        public ListWriter newList(String type) {
            return new WriteThroughListWriterImpl(jsonGenerator);
        }

        @Override
        public void writeValue(String type, Object value) {
            try {
                jsonGenerator.writeObject(value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void done() {
            try {
                jsonGenerator.writeEndArray();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
