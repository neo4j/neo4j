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
package org.neo4j.kernel.api;

public abstract class Specialization<GenericForm>
{
    public abstract GenericForm genericForm();

    /**
     * Implementations should check if the supplied visitor implements a specialized interface for handling this
     * specialized form, and if it does, that method should be invoked.
     *
     * Only if the supplied visitor does not support the specialized form of this specialization should the
     * {@linkplain UseCase#specialize(Object) generic specialization method} be used.
     */
    public <Result, Failure extends Throwable> Result specializedFor( UseCase<GenericForm, Result, Failure> useCase ) throws Failure
    {
        return useCase.specialize( genericForm() );
    }

    public final boolean isSpecialized()
    {
        return !(this instanceof None);
    }

    public interface UseCase<GenericForm, Result, Failure extends Throwable>
    {
        Result specialize( GenericForm genericForm ) throws Failure;
    }

    public static final class None<GenericForm> extends Specialization<GenericForm>
    {
        private final GenericForm genericForm;

        public None( GenericForm genericForm )
        {
            this.genericForm = genericForm;
        }

        @Override
        public GenericForm genericForm()
        {
            return genericForm;
        }
    }
}
