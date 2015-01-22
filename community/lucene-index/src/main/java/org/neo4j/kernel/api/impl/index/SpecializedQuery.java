/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.search.Query;

import org.neo4j.graphdb.Lookup;
import org.neo4j.kernel.api.Specialization;
import org.neo4j.kernel.impl.util.LookupTransformationAdapter;

class SpecializedQuery extends Specialization<Lookup>
{
    interface User<Result> extends UseCase<Lookup, Result, RuntimeException>
    {
        Result verify( SpecializedQuery specialization );
    }

    final LuceneDocumentStructure structure;
    final Query query;

    private SpecializedQuery( LuceneDocumentStructure structure, Query query )
    {
        this.structure = structure;
        this.query = query;
    }

    @Override
    public Lookup genericForm()
    {
        return structure.genericForm( query );
    }

    @Override
    public <Result, Failure extends Throwable> Result specializedFor(
            UseCase<Lookup, Result, Failure> useCase ) throws Failure
    {
        if ( useCase instanceof User<?> )
        {
            @SuppressWarnings("unchecked")
            User<Result> user = (User<Result>) useCase;
            return user.verify( this );
        }
        return super.specializedFor( useCase );
    }

    static Lookup.Transformation<? extends Specialization<Lookup>> transformation(
            LuceneDocumentStructure structure )
    {
        return new LookupTransformationAdapter<Query, LuceneDocumentStructure, SpecializedQuery>( structure )
        {
            @Override
            public SpecializedQuery not( SpecializedQuery lookup )
            {
                return transformed( negated( lookup.query ) );
            }

            @Override
            protected SpecializedQuery transformed( Query source )
            {
                return new SpecializedQuery( transformation(), source );
            }
        };
    }
}
