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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.register.Register;

class ValueRegister extends AbstractKeyValueStore.Reader<Register.DoubleLongRegister>
{
    private final Register.DoubleLongRegister target;

    public ValueRegister( Register.DoubleLongRegister target )
    {
        this.target = target;
    }

    @Override
    protected Register.DoubleLongRegister parseValue( ReadableBuffer value )
    {
        target.write( value.getLong( 0 ), value.getLong( 8 ) );
        return target;
    }

    @Override
    protected Register.DoubleLongRegister defaultValue()
    {
        target.write( 0, 0 );
        return target;
    }
}
