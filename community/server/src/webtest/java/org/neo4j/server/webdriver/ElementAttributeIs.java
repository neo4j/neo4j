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
package org.neo4j.server.webdriver;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

public class ElementAttributeIs extends BaseMatcher<ElementReference>
{

    private final String attr;
    private final String value;
    
    public static ElementAttributeIs elementAttributeIs(String attr, String value) {
        return new ElementAttributeIs( attr, value );
    }
    
    public ElementAttributeIs(String attr, String value) {
        this.attr = attr;
        this.value = value;
    }

    @Override
    public boolean matches( Object item )
    {
        if(item instanceof ElementReference) {
            String currentValue = ((ElementReference)item).getAttribute(attr);
            if ( (currentValue == null && value == null) || (currentValue != null && currentValue.matches(value))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "Element attribute "+ attr +" should match "+ value +"." );
    }

}