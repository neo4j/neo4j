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
  ['neo4j/webadmin/utils/ItemUrlResolver'
   './visualizationProfile'
   '../models/VisualizationProfile'
   '../models/StyleRule'
   './StyleRuleView'
   'ribcage/View'
   'lib/amd/jQuery'
   'lib/amd/jQuery.sortable'], 
  (ItemUrlResolver, template, VisualizationProfile, StyleRule, StyleRuleView, View, $) ->
  
    class VisualizationProfileView extends View

      events : 
        "click button.save" : "save"
        "click button.cancel" : "cancel"
        "click button.addStyleRule" : "addStyleRule"

      initialize : (opts) =>
        
        @profiles = opts.dataBrowserSettings.getVisualizationProfiles()
        @settings = opts.dataBrowserSettings
        @browserState = opts.dataBrowserState
        @styleViews = []

      save : () =>
        name = $('#profile-name',@el).val()
        
        if name.length == 0
          alert "Please enter a name for this profile."
          return
          
        for ruleView in @styleRuleViews
          if not ruleView.validates()
            alert "There are errors in one or more of your style rules, please fix those before saving."
            return
        
        @profile.setName name
        @_updateRuleOrderFromUI()
        if @isInCreateMode
          @profiles.add @profile
          @settings.setCurrentVisualizationProfile(@profile.id)
        @profile.save()

        Backbone.history.navigate('#/data/search/' + @browserState.getQuery(), true );

      cancel : () =>
        Backbone.history.navigate('#/data/search/' + @browserState.getQuery(), true );

      addStyleRule : () =>
        rule = new StyleRule()
        rule.setOrder @profile.styleRules.size()
        @profile.styleRules.addLast rule
        @addStyleRuleElement rule
        
      addStyleRuleElement : (rule) ->
        view = new StyleRuleView( rule : rule, rules:@profile.styleRules )
        
        @styleRuleViews.push view
        
        li = $ view.render().el
        li.attr('id', "styleRule_#{rule.getOrder()}")
        @styleRuleContainer.append li
      
      render : () =>

        $(@el).html(template( name : @profile.getName(), isInCreateMode:@isInCreateMode ))
        
        @styleRuleViews = []
        @styleRuleContainer = $('.styleRules',@el)
        
        sortId = 0
        @profile.styleRules.each (rule) =>
          @addStyleRuleElement rule, sortId++
        
        @styleRuleContainer.sortable({
          handle : '.form-sort-handle'
        })
        
        return this
        
      setProfileToManage : (@profile) ->
        @setIsCreateMode false
        
      setIsCreateMode : (@isInCreateMode) ->
        if @isInCreateMode        
          @profile = new VisualizationProfile(name:"",styleRules:[{  }])
        
      hasUnsavedChanges : () ->
        @profile.name != $('#profile-name',@el).val()
        
      _updateRuleOrderFromUI : () ->
        lis =  @styleRuleContainer.children()
        order = (Number li.id.split('_')[1] for li in lis)
        
        rules = @profile.styleRules
        for i in [0...order.length]
          rules.models[i].setOrder order[i]
        rules.sort()

)
