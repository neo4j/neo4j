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
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"headline-bar\"><ul class=\"dashboard-chart-tabs tabs\"><li><button value=\"primitives\" class=\"switch-dashboard-chart\">Primitives</button></li><li><button value=\"memory\" class=\"switch-dashboard-chart\">Memory</button></li></ul><ul class=\"dashboard-zoom-tabs tabs\"><li><button value=\"year\" class=\"switch-dashboard-zoom\">Year</button></li><li><button value=\"month\" class=\"switch-dashboard-zoom\">One month</button></li><li><button value=\"week\" class=\"switch-dashboard-zoom\">One week</button></li><li><button value=\"day\" class=\"switch-dashboard-zoom\">One day</button></li><li><button value=\"six_hours\" class=\"switch-dashboard-zoom\">6 hours</button></li><li><button value=\"thirty_minutes\" class=\"switch-dashboard-zoom\">30 minutes</button></li></ul><div class=\"break\"></div></div><div id=\"monitor-chart-wrap\"><div id=\"monitor-chart\"></div></div><div class=\"footer-bar\"></div>";}}; });