
from neo4j.webadmin import DashboardController
from neo4j.webadmin.views import BaseView

from lib import jquery, underscore, backbone

$(document).ready ->

  baseview = new BaseView(el:$("body"))
  baseview.render()

  new DashboardController

  Backbone.history.start()
