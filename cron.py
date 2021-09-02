import redis, config, mysql.connector

cnx = mysql.connector.connect(user = config.mysql["user"], password = config.mysql["password"], host = config.mysql["host"], database = config.mysql["db"], autocommit = True)
SQL = cnx.cursor()

if not SQL: raise Exception('Could not connect to SQL.')

r = redis.Redis(host='localhost', port=6379, db=0)

def calculateRanksAuto():
    os.system("redis-cli --scan --pattern ripple:leaderboard_auto:* | xargs redis-cli del")

    for gamemode in ['std', 'taiko', 'ctb', 'mania']:
        print(f'Mode: {gamemode}')

        SQL.execute('SELECT auto_stats.id, auto_stats.pp_{gm}, auto_stats.country, users.latest_activity FROM auto_stats LEFT JOIN users ON users.id = auto_stats.id WHERE auto_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))

        for row in SQL.fetchall():
            userID       = int(row[0])
            pp           = float(row[1])
            country      = row[2].lower()

            r.zadd(f'ripple:leaderboard_auto:{gamemode}', userID, pp)

            if country != 'xx':
                r.zincrby('hanayo:country_list', country, 1)
                r.zadd(f'ripple:leaderboard_auto:{gamemode}:{country}', userID, pp)

def calculateRanksRelax():
    os.system("redis-cli --scan --pattern ripple:leaderboard_relax:* | xargs redis-cli del")

    for gamemode in ['std', 'taiko', 'ctb', 'mania']:
        print(f'Mode: {gamemode}')

        SQL.execute('SELECT auto_stats.id, auto_stats.pp_{gm}, auto_stats.country, users.latest_activity FROM auto_stats LEFT JOIN users ON users.id = auto_stats.id WHERE auto_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))

        for row in SQL.fetchall():
            userID       = int(row[0])
            pp           = float(row[1])
            country      = row[2].lower()

            r.zadd(f'ripple:leaderboard_relax:{gamemode}', userID, pp)

            if country != 'xx':
                r.zincrby('hanayo:country_list', country, 1)
                r.zadd(f'ripple:leaderboard_relax:{gamemode}:{country}', userID, pp)

def calculateRanks():
    os.system("redis-cli --scan --pattern ripple:leaderboard:* | xargs redis-cli del")

    for gamemode in ['std', 'taiko', 'ctb', 'mania']:
        print(f'Mode: {gamemode}')

        SQL.execute('SELECT auto_stats.id, auto_stats.pp_{gm}, auto_stats.country, users.latest_activity FROM auto_stats LEFT JOIN users ON users.id = auto_stats.id WHERE auto_stats.pp_{gm} > 0 AND users.privileges & 1 ORDER BY pp_{gm} DESC'.format(gm=gamemode))

        for row in SQL.fetchall():
            userID       = int(row[0])
            pp           = float(row[1])
            country      = row[2].lower()

            r.zadd(f'ripple:leaderboard:{gamemode}', userID, pp)

            if country != 'xx':
                r.zincrby('hanayo:country_list', country, 1)
                r.zadd(f'ripple:leaderboard:{gamemode}:{country}', userID, pp)
