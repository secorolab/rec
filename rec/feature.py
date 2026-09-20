class Feature:
    def __init__(self, name, **kwargs):
        self.name = name

    # Decorators
    def capture(self, function):
        pass

    def config(self, function):
        pass

    # Public methods
    def add_config(self, config, **kwargs):
        pass

    def add_named_config(self, name, config, **kwargs):
        pass
