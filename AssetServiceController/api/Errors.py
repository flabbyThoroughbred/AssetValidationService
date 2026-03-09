"""
Need a special catch-all case for database errors for the assetSvc API to
catch.
"""
class DatabaseError(BaseException):    
    def __init__(self, msg):
        super().__init__(msg)