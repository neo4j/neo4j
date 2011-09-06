/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;

public class ElementVisible extends BaseMatcher<WebDriver>
{

    private final By by;
    
    public static ElementVisible elementVisible(By by) {
        return new ElementVisible(by);
    }
    
    public ElementVisible(By by) {
        this.by = by;
    }
    
    @Override
    public boolean matches( Object item )
    {
        if(item instanceof WebDriver) {
            WebDriver d = (WebDriver)item;
            try { 
                return ! d.findElement( by ).getCssValue( "display" ).equals( "none" );
            } catch(NoSuchElementException e) {
                return false;
            } catch(StaleElementReferenceException e) {
                return false;
            }
        }
        return false;
    }

    @Override
    public void describeTo( Description description )
    {
        description.appendText( "Element should be visible." );
    }

}
