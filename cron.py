import redis
import mysql.connector
from mysql.connector import errorcode
import time
import os
import sys
import config

VERSION = 1.29

CYAN		= '\033[96m'
MAGENTA     = '\033[95m'
YELLOW 		= '\033[93m'
GREEN 		= '\033[92m'
RED 		= '\033[91m'
ENDC 		= '\033[0m'


try:
    cnx = mysql.connector.connect(
        user       = config.mysql["user"],
        password   = config.mysql["password"],
        host       = config.mysql["host"],
        database   = config.mysql["db"],
        autocommit = True)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        raise Exception('Something is wrong with your username or password.')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        raise Exception('Database does not exist.')
    else:
        raise Exception(err)
else:
    SQL = cnx.cursor()

if not SQL: raise Exception('Could not connect to SQL.')

r = redis.Redis(host='localhost', port=6379, db=0)

def calculateRanks(): # Calculate hanayo ranks based off db pp values.
    print(f'{CYAN}-> Calculating ranks for all users in all gamemodes.{ENDC}')
    t_start = time.time()

    # do not flush as it'll break "Online Users" on hanayo.
    # r.flushall() # Flush current set (removes restricted players).
    os.system("redis-cli --scan --pattern ripple:leaderboard_auto:* | xargs redis-cli del")
    os.system("redis-cli --scan --pattern ripple:leaderboard_relax:* | xargs redis-cli del")
    os.system("redis-cli --scan --pattern ripple:leaderboard:* | xargs redis-cli del")


    for relax in range(2):
        print(f'Calculating {"Relax" if relax else "Vanilla"}.')
        for gamemode in ['std', 'taiko', 'ctb', 'mania']:
            print(f'Mode: {gamemode}')

            if relax:
                SQL.execute('SELECT rx_stats.id, rx_stats.pp_{gm}, rx_stats.country, users.latest_activity FROM rx_stats LEFT JOIN users ON users.id = rx_stats.id WHERE rx_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))
            else:
                SQL.execute('SELECT users_stats.id, users_stats.pp_{gm}, users_stats.country, users.latest_activity FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))

            currentTime = int(time.time())
            for row in SQL.fetchall():
                userID       = int(row[0])
                pp           = float(row[1])
                country      = row[2].lower()
                daysInactive = (currentTime - int(row[3])) / 60 / 60 / 24
                
                if daysInactive > 60:
                    continue

                if relax:
                    r.zadd(f'ripple:leaderboard_relax:{gamemode}', userID, pp)
                else:
                    r.zadd(f'ripple:leaderboard:{gamemode}', userID, pp)

                if country != 'xx':
                    r.zincrby('hanayo:country_list', country, 1)

                    r.zadd(f'ripple:leaderboard_relax:{gamemode}:{country}', userID, pp)
                    r.zadd(f'ripple:leaderboard:{gamemode}:{country}', userID, pp)

    print(f'{GREEN}-> Successfully completed rank calculations.\n{MAGENTA}Time: {time.time() - t_start:.2f} seconds.{ENDC}')
    return True

def calculateRanksA(): # Calculate hanayo ranks based off db pp values.
    print(f'{CYAN}-> Calculating ranks for all users in all gamemodes.{ENDC}')
    t_start = time.time()

    # do not flush as it'll break "Online Users" on hanayo.
    # r.flushall() # Flush current set (removes restricted players).
    r.delete(r.keys("ripple:leaderboard:*"))
    r.delete(r.keys("ripple:autoboard:*"))

    for auto in range(2):
        print(f'Calculating {"auto" if auto else "Vanilla"}.')
        for gamemode in ['std', 'taiko', 'ctb', 'mania']:
            print(f'Mode: {gamemode}')

            if auto:
                SQL.execute('SELECT auto_stats.id, auto_stats.pp_{gm}, auto_stats.country, users.latest_activity FROM auto_stats LEFT JOIN users ON users.id = auto_stats.id WHERE auto_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))
            else:
                SQL.execute('SELECT users_stats.id, users_stats.pp_{gm}, users_stats.country, users.latest_activity FROM users_stats LEFT JOIN users ON users.id = users_stats.id WHERE users_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))

            currentTime = int(time.time())
            for row in SQL.fetchall():
                userID       = int(row[0])
                pp           = float(row[1])
                country      = row[2].lower()
                daysInactive = (currentTime - int(row[3])) / 60 / 60 / 24
                
                if daysInactive > 60:
                    continue

                if auto:
                    r.zadd(f'ripple:leaderboard_auto:{gamemode}', userID, pp)
                else:
                    r.zadd(f'ripple:leaderboard:{gamemode}', userID, pp)

                if country != 'xx':
                    r.zincrby('hanayo:country_list', country, 1)

                    r.zadd(f'ripple:leaderboard_auto:{gamemode}:{country}', userID, pp)
                    r.zadd(f'ripple:leaderboard:{gamemode}:{country}', userID, pp)

    print(f'{GREEN}-> Successfully completed rank calculations.\n{MAGENTA}Time: {time.time() - t_start:.2f} seconds.{ENDC}')
    return True

if __name__ == '__main__':
    print(f"{CYAN}smile - v{VERSION}.{ENDC}")
    intensive = len(sys.argv) > 1 and any(sys.argv[1].startswith(x) for x in ['t', 'y', '1'])
    t_start = time.time()
    # lol this is cursed code right here
    if calculateRanks(): print()
    if calculateRanksA(): print()

    print(f'{GREEN}-> Cronjob execution completed.\n{MAGENTA}Time: {time.time() - t_start:.2f} seconds.{ENDC}')
