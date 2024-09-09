import sqlite3

class Base():
    def __init__(self, dbPath):
        self.__dbPath = dbPath

    def connect(self):
        return sqlite3.connect(self.__dbPath)
