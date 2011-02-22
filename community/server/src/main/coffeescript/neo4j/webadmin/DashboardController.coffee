
define ['./views/BaseView', 'lib/backbone'], (BaseView) ->
  
  class ApplicationController extends Backbone.Controller
    routes : 
      "" : "dashboard"
      "/someshit" : "someshit"
      
    dashboard : ->
      
    someshit : ->
      alert "WOOT"
