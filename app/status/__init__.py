from flask import Blueprint

status_blue = Blueprint('status', __name__)
from . import views