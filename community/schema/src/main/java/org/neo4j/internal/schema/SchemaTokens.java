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
package org.neo4j.internal.schema;

import java.util.Collection;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.api.set.MutableSet;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeLabelExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipEndpointLabel;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType;
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText;
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector;

public record SchemaTokens(
        ImmutableSet<String> labels, ImmutableSet<String> relationships, ImmutableSet<String> properties) {
    /**
     * @param indexes the index commands whose tokens should be collected
     * @param constraints the constraint commands whose tokens should be collected
     * @return all the various tokens required by the provided schema commands
     */
    public static SchemaTokens collect(
            Collection<IndexCommand.Create> indexes, Collection<ConstraintCommand.Create> constraints) {
        MutableSet<String> labels = Sets.mutable.empty();
        MutableSet<String> relationships = Sets.mutable.empty();
        MutableSet<String> properties = Sets.mutable.empty();
        collectIndexes(labels, relationships, properties, indexes);
        collectConstraints(labels, relationships, properties, constraints);
        return new SchemaTokens(labels.toImmutable(), relationships.toImmutable(), properties.toImmutable());
    }

    static void collectIndexes(
            MutableSet<String> labels,
            MutableSet<String> relationships,
            MutableSet<String> properties,
            Collection<IndexCommand.Create> indexes) {
        for (IndexCommand.Create index : indexes) {
            switch (index) {
                case NodeRange nodeRange -> {
                    labels.add(nodeRange.label());
                    properties.addAll(nodeRange.properties());
                }
                case RelationshipRange relationshipRange -> {
                    relationships.add(relationshipRange.type());
                    properties.addAll(relationshipRange.properties());
                }
                case NodeText nodeText -> {
                    labels.add(nodeText.label());
                    properties.add(nodeText.property());
                }
                case RelationshipText relationshipText -> {
                    relationships.add(relationshipText.type());
                    properties.add(relationshipText.property());
                }
                case NodePoint nodePoint -> {
                    labels.add(nodePoint.label());
                    properties.add(nodePoint.property());
                }
                case RelationshipPoint relationshipPoint -> {
                    relationships.add(relationshipPoint.type());
                    properties.add(relationshipPoint.property());
                }
                case NodeFulltext nodeFulltext -> {
                    labels.addAll(nodeFulltext.labels());
                    properties.addAll(nodeFulltext.properties());
                }
                case RelationshipFulltext relationshipFulltext -> {
                    relationships.addAll(relationshipFulltext.types());
                    properties.addAll(relationshipFulltext.properties());
                }
                case NodeVector nodeVector -> {
                    labels.addAll(nodeVector.labels());
                    properties.add(nodeVector.property());
                    properties.addAll(nodeVector.additionalProperties());
                }
                case RelationshipVector relationshipVector -> {
                    relationships.addAll(relationshipVector.types());
                    properties.add(relationshipVector.property());
                    properties.addAll(relationshipVector.additionalProperties());
                }
                case RelationshipLookup relationshipLookup -> {
                    // No tokens associated with a relationship lookup index.
                }
                case NodeLookup nodeLookup -> {
                    // No tokens associated with a node lookup index.
                }
            }
        }
    }

    static void collectConstraints(
            MutableSet<String> labels,
            MutableSet<String> relationships,
            MutableSet<String> properties,
            Collection<ConstraintCommand.Create> constraints) {
        for (ConstraintCommand.Create constraint : constraints) {
            switch (constraint) {
                case NodeKey constraintCommand -> {
                    labels.add(constraintCommand.label());
                    properties.addAll(constraintCommand.properties());
                }
                case RelationshipKey constraintCommand -> {
                    relationships.add(constraintCommand.type());
                    properties.addAll(constraintCommand.properties());
                }
                case NodeUniqueness constraintCommand -> {
                    labels.add(constraintCommand.label());
                    properties.addAll(constraintCommand.properties());
                }
                case RelationshipUniqueness constraintCommand -> {
                    relationships.add(constraintCommand.type());
                    properties.addAll(constraintCommand.properties());
                }
                case NodeExistence constraintCommand -> {
                    labels.add(constraintCommand.label());
                    properties.add(constraintCommand.property());
                }
                case RelationshipExistence constraintCommand -> {
                    relationships.add(constraintCommand.type());
                    properties.add(constraintCommand.property());
                }
                case NodePropertyType constraintCommand -> {
                    labels.add(constraintCommand.label());
                    properties.add(constraintCommand.property());
                }
                case RelationshipPropertyType constraintCommand -> {
                    relationships.add(constraintCommand.type());
                    properties.add(constraintCommand.property());
                }
                case NodeLabelExistence nodeLabelExistence -> {
                    labels.add(nodeLabelExistence.label());
                    labels.add(nodeLabelExistence.requiredLabel());
                }
                case RelationshipEndpointLabel relationshipEndpointLabel -> {
                    relationships.add(relationshipEndpointLabel.type());
                    labels.add(relationshipEndpointLabel.requiredLabel());
                }
            }
        }
    }

    /**
     * @return <code>true</code> if there are no label, relationship or property tokens referenced in the schema commands
     */
    public boolean isEmpty() {
        return labels.isEmpty() && relationships().isEmpty() && properties.isEmpty();
    }
}
