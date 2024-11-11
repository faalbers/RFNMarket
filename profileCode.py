import cProfile, sys

# import rfnmarketTest, databaseDev, databaseSpeedTest
import rfnmarketTest, rfnmarketTest

original_stdout = sys.stdout 
with open("profile.txt", "w") as f:
    sys.stdout = f  # Redirect standard output to the file
    cProfile.run('rfnmarketTest.runthis()', sort='cumtime')
    sys.stdout = original_stdout  # Restore the original standard output
