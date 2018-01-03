###
Copyright (c) 2002-2018 "Neo Technology,"
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
  ['./guide'
   'ribcage/storage/CookieStorage'
   'lib/amd/jQuery'
   'neo4j/webadmin/modules/baseui/models/MainMenuModel'
   'lib/amd/Deck'
   ], 
  (template, CookieStorage, $, MainMenuModel, Deck) ->
    
    class GuideDeck

      @COOKIE_NAME: "guideShown@@neo4j.version@@"

      constructor : ->
        @cookies = new CookieStorage

      init : ->
        # Show boot screen for flashiness
        @guide = $(template())
        $("body").append(@guide)

        $('.close-guide').click( (event) =>
          @hide()
        )

        $('.start-guide').click( (event) =>
          @show()
        )

        if not @hasBeenShownForThisSession()
          @show("/webadmin/deck/welcome.html")
          setTimeout () => @show();, 
          3000

      hasBeenShownForThisSession : ->
        @cookies.get(@COOKIE_NAME) != null

      show : (deckUrl) ->
        deckUrl = deckUrl ? "/webadmin/deck/guide.html"
        $('.deck-container').load(deckUrl, (responseTxt,statusTxt,xhr) =>
            Deck('.slide');
            # Deck('enableScale');
            $('.deck-url').click( ( event ) =>
                event.preventDefault();
                @show($(event.target).attr("href"));
            );
        )
        @guide.fadeIn(200)
        @cookies.set(@COOKIE_NAME, "1")

        
      hide : ->
        @clearDeck()
        @guide.fadeOut(400, => 
          @cookies.set(@COOKIE_NAME, "0")
        )

      clearDeck : ->
        $(document).unbind('keydown.deckscale')
        $(document).unbind('keydown.deck')
        
        
)
