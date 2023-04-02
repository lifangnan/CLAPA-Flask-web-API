from flask import Blueprint

auth_blue = Blueprint('auth', __name__, url_prefix='/auth')
from . import views