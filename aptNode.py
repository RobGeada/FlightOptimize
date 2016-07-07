class aptNode:
    def __init__(self,ID,pos,f,g,h):
        self.path = []
        self.ID = ID
        self.pos= pos
        self.f = f
        self.g = g
        self.h = h

    def __repr__(self):
        return "Airport {}: fgh=({},{},{})".format(repr(self.ID),repr(self.f),repr(self.g),repr(self.h))

    def add_apt(self,apt):
        self.path.append(apt)

    def setf(self):
        self.f = self.g + self.h

    def setfgh(self,arr):
        self.f = arr[0]
        self.g = arr[1]
        self.h = arr[2]
