from .basehandler import DefaultHandler
from tornado import escape

# 统一的登录处理，根据用户选择登录方式，匹配相应的认证模块
class LoginHandler(DefaultHandler):
    # 跳转到登录界面，要合理实现跳转回到调用页面
    async def get(self):
        self.set_header("Content-Type", "text/html;charset=utf-8")        
        next=self.arguments.get("next","/")
        self.render("login.html", next=next)

    # 提交登录数据，完成登录，保存登录结果
    async def post(self):        
        # 进行认证，获取用户信息
        username = self.arguments["username"]
        password = self.arguments["password"]
        #保存信息-后续get_current_user会重新获取
        self.set_secure_cookie("user", username)
        #跳转到相应页面
        forward = self.arguments.get("next","/")
        self.redirect(forward)

handler_map = [
    (r'/login', LoginHandler),
]
