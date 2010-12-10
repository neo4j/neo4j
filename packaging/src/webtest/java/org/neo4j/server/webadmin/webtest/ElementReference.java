/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.webadmin.webtest;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.NoSuchElementException;
import org.openqa.selenium.RenderedWebElement;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

/**
 * This is used rather than a direct
 * (RenderedWebElement) reference, because our UI renders a lot, and such
 * references often become stale. This allows code that encounters
 * StaleElementReferenceException to simply reload the element.
 * 
 * This class also provides a subset of the RenderedWebElement API, which
 * abstracts away StaleElementReferenceException.
 */
public class ElementReference {

	protected By selector;
	protected WebDriver webDriver;
	
	public ElementReference(WebDriver webDriver, By selector) {
		this.webDriver = webDriver;
		this.selector = selector;
	}
	
	/**
	 * Attempts to fetch this element. If no element can be found, 
	 * fetching the element will be retried until one is found, or
	 * 10 seconds have passed.
	 * 
	 * @return
	 */
	public RenderedWebElement getElement() {
		return getElement(selector);
    }
	
	/**
	 * Attempts to fetch an element. If no element can be found, 
	 * fetching will be retried until one is found, or
	 * 10 seconds have passed.
	 * 
	 * @return
	 */
	public RenderedWebElement getElement(By selector) {
		long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            try {
                return (RenderedWebElement) webDriver.findElement(selector);
            } catch (NoSuchElementException ex) {
                try {
					Thread.sleep(13);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
            }
        }

        throw new NoSuchElementException("Unable to locate element: " + selector.toString());
    }
	
	public RenderedWebElement findElement(By by) {
		try {
    		return (RenderedWebElement)this.getElement().findElement(by);
    	} catch (StaleElementReferenceException e) {
    		return this.findElement(by);
        }
	}
	
	public List<WebElement> findElements(By by) {
		try {
    		return this.getElement().findElements(by);
    	} catch (StaleElementReferenceException e) {
    		return this.findElements(by);
        }
	}

	public String getAttribute( String attributeName ) {
		try {
    		return this.getElement().getAttribute(attributeName);
    	} catch (StaleElementReferenceException e) {
    		return this.getAttribute(attributeName);
        }
	}
	
	public void click() {
		try {
			this.getElement().click();
		} catch (StaleElementReferenceException e) {
			this.click();
	    }
	}
	
	public String getValueOfCssProperty(String cssProperty) {
		try {
    		return this.getElement().getValueOfCssProperty(cssProperty);
    	} catch (StaleElementReferenceException e) {
    		return this.getValueOfCssProperty(cssProperty);
        }
	}
	
	public void sendKeys(CharSequence ... keysToSend) {
		try {
    		this.getElement().sendKeys(keysToSend);
    	} catch (StaleElementReferenceException e) {
    		this.sendKeys(keysToSend);
        }
	}
	
	public String getValue() {
		try {
    		return this.getElement().getValue();
    	} catch (StaleElementReferenceException e) {
    		return this.getValue();
        }
	}
	
	public String getText() {
		try {
    		return this.getElement().getText();
    	} catch (StaleElementReferenceException e) {
    		return this.getText();
        }
	}
	
	public void clear() {
		try {
			this.getElement().clear();
    	} catch (StaleElementReferenceException e) {
    		this.clear();
        }
	}
	
	public void waitUntilVisible() {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            if (this.getValueOfCssProperty("display") != "none") {
                return;
            }
        }

        throw new RuntimeException("Element did not become visible within a reasonable time. Element was: " + this.getElement().toString());
    }

	public void waitForAttributeToBe(String attributeName, String expectedValue) {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            if (this.getAttribute(attributeName) == expectedValue) {
                return;
            }
        }

        throw new RuntimeException("Element did not become visible within a reasonable time. Element was: " + this.getElement().toString());
    }

	public void waitForAttributeToChangeFrom(String attributeName, String currentValue) {
        long end = System.currentTimeMillis() + 10000;
        while (System.currentTimeMillis() < end) {
            if (this.getAttribute(attributeName) != currentValue) {
                return;
            }
        }

        throw new RuntimeException("Element attribute did not change within a reasonable time. Element was: " + this.getElement().toString());
    }

}
