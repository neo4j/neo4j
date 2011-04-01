/*
 * Copyright (c) 2002-2011 "Neo Technology,"
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
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar pad\"><div class=\"title\"><h3>" + 
"Relationship " + item.getId() + 
"</h3><p class=\"small\">" + 
item.getSelf() + 
"</p></div><ul class=\"button-bar item-controls\"><li><button disabled=\"true\" class=\"data-save-properties button\">Saved</button></li><li><button class=\"data-delete-item button\">Delete</button></li></ul><ul class=\"relationship-meta\"><li><a href=\"#/data/search/" +
item.getStartId() +
"/\" class=\"micro-button\">" + 
"Node " + item.getStartId() + 
"</a></li><li class=\"type\">" + 
item.getItem().getType() + 
"</li><li><a href=\"#/data/search/" +
item.getEndId() +
"/\" class=\"micro-button\">" + 
"Node " + item.getEndId() + 
"</a></li></ul><div class=\"break\"></div></div><div class=\"properties\"></div>";}}; });