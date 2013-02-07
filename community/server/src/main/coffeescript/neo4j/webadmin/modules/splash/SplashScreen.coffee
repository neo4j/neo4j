###
Copyright (c) 2002-2011 "Neo Technology,"
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

define( 
  ['./splash'
   'ribcage/storage/CookieStorage'
   'lib/amd/jQuery'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'lib/amd/Deck'
   ], 
  (template, CookieStorage, $, MainMenuModel, Deck) ->
    
    class Splash

      constructor : ->
        @cookies = new CookieStorage

      init : ->
        # Show boot screen for flashiness
        @splash = $(template())
        $("body").append(@splash)

        $('.close-guide').click( (event) =>
          @hide()
        )
        if not @hasBeenShownForThisSession()
          @show()

      hasBeenShownForThisSession : ->
        @cookies.get("splashShown1.6") != null

      show : (deckUrl) ->
        deckUrl = (if (typeof deckUrl is "undefined") then "/webadmin/deck/guide.html" else deckUrl)
        $('.deck-container').load(deckUrl, (responseTxt,statusTxt,xhr) =>
            Deck('.slide');
            $('.deck-url').click( ( event ) =>
                event.preventDefault();
                @show($(event.target).attr("href"));
            );
        )
        @splash.fadeIn(200)
        @cookies.set("splashShown1.6", "1")

        
      hide : ->
        @splash.fadeOut(400, => 
          @cookies.set("splashShown1.6", "0")
        )
        
)
