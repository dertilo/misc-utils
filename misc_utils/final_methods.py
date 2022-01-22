# in contrary to mypy using typing.final Annotation, the following does check+error at runtime!

# TODO: not really good solution, cause it prevents from using multi-inheritance! see: https://stackoverflow.com/questions/36152062/how-can-i-use-multiple-inheritance-with-a-metaclass
class Access(type):
    # https://stackoverflow.com/questions/3948873/prevent-function-overriding-in-python

    __SENTINEL = object()

    def __new__(mcs, name, bases, class_dict):
        private = {
            key
            for base in bases
            for key, value in vars(base).items()
            if callable(value) and mcs.__is_final(value)
        }
        if any(key in private for key in class_dict):
            raise RuntimeError("certain methods may not be overridden")
        return super().__new__(mcs, name, bases, class_dict)

    @classmethod
    def __is_final(mcs, method):
        try:
            return method.__final is mcs.__SENTINEL
        except AttributeError:
            return False

    @classmethod
    def final(mcs, method):
        method.__final = mcs.__SENTINEL
        return method


# class Parent(metaclass=Access):
#
#     @Access.final
#     def do_something(self):
#         """This is where some seriously important stuff goes on."""
#         pass
