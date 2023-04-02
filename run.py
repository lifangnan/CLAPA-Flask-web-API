from app import create_app

app = create_app()
if __name__ == '__main__':
    # 直接用python运行flask服务
    # app.run(port='5055')
    
    app.run(debug=True,host='0.0.0.0', port='5055')