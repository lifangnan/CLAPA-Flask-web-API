from flask import Blueprint

devicefiles_blue = Blueprint('devicefiles', __name__, url_prefix='/devicefiles')
from . import views