/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking;

import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.IndexRule;

public class SchemaRuleUtil
{
    private SchemaRuleUtil()
    {
    }

    public static ConstraintRule uniquenessConstraintRule( long ruleId, int labelId, int propertyId, long indexId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.uniqueForLabel( labelId, propertyId ), indexId );
    }

    public static ConstraintRule nodePropertyExistenceConstraintRule( long ruleId, int labelId, int propertyId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.existsForLabel( labelId, propertyId ) );
    }

    public static ConstraintRule relPropertyExistenceConstraintRule( long ruleId, int labelId, int propertyId )
    {
        return ConstraintRule.constraintRule( ruleId,
                ConstraintDescriptorFactory.existsForRelType( labelId, propertyId ) );
    }

    public static IndexRule indexRule( long ruleId, int labelId, int propertyId, SchemaIndexProvider.Descriptor
            descriptor )
    {
        return IndexRule.indexRule( ruleId, IndexDescriptorFactory.forLabel( labelId, propertyId ), descriptor );
    }

    public static IndexRule constraintIndexRule( long ruleId, int labelId, int propertyId,
            SchemaIndexProvider.Descriptor descriptor, long constraintId )
    {
        return IndexRule.constraintIndexRule( ruleId, IndexDescriptorFactory.uniqueForLabel( labelId, propertyId ),
                descriptor, constraintId );
    }

    public static IndexRule constraintIndexRule( long ruleId, int labelId, int propertyId,
            SchemaIndexProvider.Descriptor descriptor )
    {
        return IndexRule.indexRule( ruleId, IndexDescriptorFactory.uniqueForLabel( labelId, propertyId ),
                descriptor );
    }
}
