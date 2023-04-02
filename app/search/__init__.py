from flask import Blueprint

search_blue = Blueprint('search', __name__, url_prefix='/search')
from . import views