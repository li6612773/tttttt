class _constant:
    class ConstError(TypeError): pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" % name)
        self.__dict__[name] = value


constant = _constant()
constant.proToken = "3841b268c623d6c28f766ef1ffdd0b40737af355759ef44488f8e600"
constant.dbPath = "mysql+pymysql://root:123@192.168.3.13:3306/qtdb"


def get_pro_token():
    # 返回全局设置Token
    return constant.proToken


def get_db_path():
    # 返回全局设置dbPath
    return constant.dbPath




