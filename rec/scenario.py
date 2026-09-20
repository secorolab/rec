from rec.run import Run
from rec.feature import Feature


class Scenario(Feature):
    def __init__(self, name, ingredients: list, **kwargs):
        self.name = name
        self.ingredients = ingredients
        self.observers = kwargs.get("observers", [])
        self.current_run = None

    def run(self):
        # Create a run object
        self.current_run = Run(observers=self.observers)

        # Call it so it runs
        self.current_run.run()
        return self.current_run
