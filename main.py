import subprocess

# run block rewards
print('Running Block Reward Script')
subprocess.run(["python3", "blockRewards.py"])

print('Running Staked Realised Value Script')
subprocess.run(["python3", "SRV_USD.py"])

print('Running Ticket and Votes Counts')
subprocess.run(["python3", "TicketsVotesCount.py"])

print('Running Supply')
subprocess.run(["python3", "Supply.py"])

print('Running Block versions')
subprocess.run(["python3", "blockVersions.py"])

print('Running Vote versions')
subprocess.run(["python3", "voteVersions.py"])

print('treasury data')
subprocess.run(["python3", "treasury.py"])

print('hashrate data')
subprocess.run(["python3", "nethash.py"])

print('pow Reward Dist')
subprocess.run(["python3", "powRewardDist.py"])

print('BTC SRV')
subprocess.run(["python3", "SRV_BTC.py"])
