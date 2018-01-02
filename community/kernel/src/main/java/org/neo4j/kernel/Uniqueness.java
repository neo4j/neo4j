/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.graphdb.traversal.UniquenessFilter;

/**
 * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
 */
public enum Uniqueness implements UniquenessFactory
{
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    NODE_GLOBAL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.NODE_GLOBAL.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    NODE_PATH
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.NODE_PATH.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     *@deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    NODE_RECENT
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.NODE_RECENT.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    NODE_LEVEL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.NODE_LEVEL.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    RELATIONSHIP_GLOBAL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_GLOBAL.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    RELATIONSHIP_PATH
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_PATH.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return false;
        }
    },
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    RELATIONSHIP_RECENT
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_RECENT.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    RELATIONSHIP_LEVEL
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.RELATIONSHIP_LEVEL.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    },
    
    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.Uniqueness}
     */
    NONE
    {
        public UniquenessFilter create( Object optionalParameter )
        {
            return org.neo4j.graphdb.traversal.Uniqueness.NONE.create(optionalParameter);
        }

        public boolean eagerStartBranches()
        {
            return true;
        }
    };
}
