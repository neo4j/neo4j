package org.neo4j.kernel;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.ConstraintDefinition;

public interface InternalConstraintActions
{
    ConstraintDefinition createPropertyUniquenessConstraint( Label label, String propertyKey );
    
    void dropPropertyUniquenessConstraint( Label label, String propertyKey );
}