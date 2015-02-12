###!
Copyright (c) 2002-2015 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

angular.module('neo4jApp.directives')
  .directive('fancyLogo', [
    '$window'
    ($window) ->
      template: '<h1>fancified</h1>'
      link: (scope, element, attrs, ctrl) ->
        element.html(
          if Modernizr.inlinesvg
            '<span class="ball one"/><span class="ball two"/><span class="ball three"/>'
          else
            """
            <svg viewBox="41 29 125 154" width="125pt" height="154pt"><defs><pattern id="img1" patternUnits="objectBoundingBox" width="90" height="90"><image href="images/faces/abk.jpg" x="0" y="0" width="64" height="64"></image></pattern></defs><g class="logo" stroke="none" stroke-opacity="1" stroke-dasharray="none" fill-opacity="1"><circle class="node" cx="129.63533" cy="84.374286" r="32.365616" fill="#fad000"></circle><circle class="node" cx="62.714058" cy="50.834676" r="18.714163" fill="#fad000"></circle><circle class="node" cx="83.102398" cy="152.22447" r="26.895987" fill="#fad000"></circle><circle class="relationship" cx="91.557016" cy="45.320086" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="104.57301" cy="49.659258" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="55.755746" cy="78.59023" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="55.755746" cy="92.690676" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="58.64808" cy="108.24096" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="65.87916" cy="121.25976" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="118.67652" cy="138.25673" r="5.0627656" fill="#ff4907" stroke="none"></circle><circle class="relationship" cx="127.35707" cy="127.40609" r="5.0627656" fill="#ff4907" stroke="none"></circle><path class="swish" d="M 157.176255 67.359654 C 155.88412 65.2721 154.33242 63.29959 152.52118 61.488342 C 139.88167 48.84871 119.389024 48.84871 106.74953 61.488342 C 94.109954 74.127904 94.109954 94.620657 106.74953 107.260246 C 107.89654 108.40725 109.10819 109.45017 110.37279 110.38901 C 102.64778 97.90879 104.199466 81.316687 115.027814 70.488345 C 126.520325 58.995706 144.50541 57.952786 157.176255 67.35964 Z" fill="#f5aa00"></path><path class="swish" d="M 78.48786 41.29777 C 77.75747 40.117761 76.88036 39.00278 75.856537 37.978957 C 68.711942 30.834292 57.12829 30.834292 49.983703 37.978957 C 42.839068 45.123583 42.839068 56.707297 49.983703 63.85194 C 50.63206 64.500294 51.316958 65.089815 52.031784 65.6205 C 47.665153 58.565944 48.542256 49.187108 54.663076 43.06629 C 61.159322 36.569972 71.325554 35.980452 78.48786 41.297761 Z" fill="#f5aa00"></path><path class="swish" d="M 104.91025 138.61693 C 103.88164 136.955135 102.64641 135.384915 101.20457 133.94307 C 91.142876 123.88128 74.829684 123.88128 64.768004 133.94307 C 54.706255 144.00481 54.706255 160.31808 64.768004 170.37984 C 65.68108 171.29292 66.64562 172.12314 67.652304 172.8705 C 61.502802 162.93561 62.73802 149.727445 71.35794 141.10753 C 80.506564 131.958805 94.82361 131.12859 104.91025 138.61692 Z" fill="#f5aa00"></path><circle class="node-outline" stroke-linecap="round" stroke-linejoin="round" stroke-width="3" fill="none" cx="129.63533" cy="84.374286" r="32.365616" stroke="#eb7f00"></circle><circle class="node-outline" stroke-linecap="round" stroke-linejoin="round" stroke-width="3" fill="none" cx="62.714058" cy="50.834676" r="18.714163" stroke="#eb7f00"></circle><circle class="node-outline" stroke-linecap="round" stroke-linejoin="round" stroke-width="3" fill="none" cx="83.102394" cy="152.22448" r="26.895992" stroke="#eb7f00"></circle></g></svg>
            """
        )
  ])
