import asyncio, os, aiomysql, uvloop, config, cron
from cmyui import AsyncSQLPool

async def main(CYAN='\033[96m', ENDC='\033[0m', botUser=config.botID, db=AsyncSQLPool()):
	await db.connect(config.mysql)
	async with db.pool.acquire() as conn:
		async with conn.cursor(aiomysql.DictCursor) as update_cursor:
			async with conn.cursor(aiomysql.DictCursor) as select_cursor:
				await select_cursor.execute('SELECT id FROM users')
				async for row in select_cursor:
					userID = row['id']
					newUserID = int(userID) - 997
					if userID == botUser:
						newUserID = 1
					# Update userid values in database
					await update_cursor.execute(f'UPDATE auto_beatmap_playcount SET user_id = {int(newUserID)} WHERE user_id = {int(userID)};'
												f'UPDATE auto_stats SET id = {int(newUserID)} WHERE id = {int(userID)};'
												f'UPDATE hw_user SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE identity_tokens SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE ip_user SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE irc_tokens SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE profile_backgrounds SET uid = {int(newUserID)} WHERE uid = {int(userID)};'
												f'UPDATE rap_logs SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE remember SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE rx_beatmap_playcount SET user_id = {int(newUserID)} WHERE user_id = {int(userID)};'
												f'UPDATE rx_stats SET id = {int(newUserID)} WHERE id = {int(userID)};'
												f'UPDATE scores SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE scores_auto SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE scores_relax SET userid = {int(newUserID)} WHERE userid = {int(userID)};'
												f'UPDATE tokens SET user = {int(newUserID)} WHERE user = {int(userID)};'
												f'UPDATE users SET id = {int(newUserID)} WHERE id = {int(userID)};'
												f'UPDATE users_achievements SET user_id = {int(newUserID)} WHERE user_id = {int(userID)};'
												f'UPDATE users_beatmap_playcount SET user_id = {int(newUserID)} WHERE user_id = {int(userID)};'
												f'UPDATE users_relationships SET user1 = {int(newUserID)} WHERE user1 = {int(userID)};'
												f'UPDATE users_relationships SET user2 = {int(newUserID)} WHERE user2 = {int(userID)};'
												f'UPDATE users_stats SET id = {int(newUserID)} WHERE id = {int(userID)};'
												f'UPDATE user_badges SET user = {int(newUserID)} WHERE user = {int(userID)};'
												f'UPDATE user_clans SET user = {int(newUserID)} WHERE user = {int(userID)};')

					# Update avatar files to match new userid values
					if os.path.isfile("{}{}.png".format(config.avatarDir, userID)):
						os.rename("{}{}.png".format(config.avatarDir, userID), "{}{}.png".format(config.avatarDir, newUserID))
					else:
						pass

					print(f'{CYAN}User ID/Avatar ID Changed | {userID} -> {newUserID}{ENDC}')

				# Change auto increment for new users registering
				row = await db.fetch('SELECT id FROM users ORDER BY id DESC LIMIT 1')
				lastID = int(row['id']) + 1
				await update_cursor.execute(f'ALTER TABLE auto_stats AUTO_INCREMENT = {int(lastID)};'
											f'ALTER TABLE rx_stats AUTO_INCREMENT = {int(lastID)};'
											f'ALTER TABLE users_stats AUTO_INCREMENT = {int(lastID)};'
											f'ALTER TABLE users AUTO_INCREMENT = {int(lastID)};')

				print(f'{CYAN}Auto Increment Changed -> {int(lastID)}{ENDC}')

				# Update leaderboards for new userid values
				cron.calculateRanks()
				cron.calculateRanksRelax()
				cron.calculateRanksAuto()

				print(f'{CYAN}Leaderboards updated for new users.{ENDC}')

uvloop.install()
asyncio.run(main())
