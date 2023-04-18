from flask import Flask

from flask_cors import CORS
from flask_jwt import JWT


import os
from dotenv import load_dotenv
def create_app():

    app = Flask(__name__)
    CORS(app, supports_credentials=True)

    load_dotenv()

    # 导入所有环境变量
    #在上面的代码示例中，我们首先使用 load_dotenv() 函数加载 .env 文件中的环境变量。然后，我们使用 os.environ 字典遍历所有的环境变量，并将它们导入到 Flask 应用程序的 config 对象中。
    for key in os.environ:
        app.config[key] = os.getenv(key)
    with app.app_context():
        from .utils import authenticate,identity
        jwt = JWT(app, authenticate, identity)
    # 导入蓝图必须放在注册蓝图上面
        from .search import search_blue
        from .auth import auth_blue
        from .user import user_blue
        from .status import status_blue
        app.register_blueprint(search_blue, url_prefix='/search')
        app.register_blueprint(auth_blue, url_prefix='/auth')
        app.register_blueprint(user_blue, url_prefix='/user')
        app.register_blueprint(status_blue, url_prefix='/')
        
    return app