class ResourceNotFoundException(Exception):
    def __init__(self, resource: str, id: int):
        self.message = f"{resource} with id {id} not found"
        super().__init__(self.message)
