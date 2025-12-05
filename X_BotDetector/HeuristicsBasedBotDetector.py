import pyodbc
import pandas as pd

conn = pyodbc.connect(
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=DJs_Laptop\SQLEXPRESS;"
    "Database=master;"
    "Trusted_Connection=yes;"
)
cursor = conn.cursor()

bot_suspects = set()

queries = {
    "tweet_intensity": """
        WITH hourly_activity AS (
            SELECT 
                t.user_id,
                u.username,
                COUNT(*) AS total_tweets,
                COUNT(DISTINCT DATEPART(HOUR, t.timestamp)) AS distinct_hours
            FROM dbo.X_Tweets t
            JOIN dbo.X_Users u ON u.user_id = t.user_id
            GROUP BY t.user_id, u.username
        )
        SELECT 
            user_id,
            username
        FROM hourly_activity
        WHERE total_tweets >= 80              
          AND total_tweets * 1.0 / distinct_hours >= 8;  

    """,
    "tweets_in_week": """
        SELECT 
            t.user_id,
            u.username
        FROM dbo.X_Tweets t
        JOIN dbo.X_Users u ON u.user_id = t.user_id
        GROUP BY 
            t.user_id,
            u.username,
            DATEADD(WEEK, DATEDIFF(WEEK, 0, t.timestamp), 0)
        HAVING COUNT(*) = 100
        ORDER BY COUNT(*) DESC;
    """,
    "engagement_deficit": """
        SELECT 
            u.user_id,
            u.username
        FROM dbo.X_Tweets t
        JOIN dbo.X_Users u ON u.user_id = t.user_id
        WHERE t.type != 'retweeted'
        GROUP BY 
            u.user_id,
            u.username,
            u.followers_count
        HAVING
            u.followers_count >= 1
        AND
            (SUM(t.like_count + t.reply_count + t.retweet_count + t.quote_count + t.bookmark_count))
             / (u.followers_count * COUNT(t.tweet_id)*1.0) < 0.002
        """
}

bot_hits = {}

for label, q in queries.items():
    print(f"Running rule: {label}")
    cursor.execute(q)
    for user_id, username in cursor.fetchall():
        if user_id not in bot_hits:
            bot_hits[user_id] = {"username": username, "score": 0, "rules": []}
        bot_hits[user_id]["score"] += 1
        bot_hits[user_id]["rules"].append(label)


total_rules = len(queries)

flagged = {
    user_id: data
    for user_id, data in bot_hits.items()
    if data["score"] == total_rules
}


df = pd.DataFrame([
    {"user_id": user, "username": data["username"], "rules_failed": ",".join(data["rules"])}
    for user, data in flagged.items()
])

update_query = "UPDATE dbo.X_BotOrNot SET is_bot = 1 WHERE user_id = ?"

for user_id, data in flagged.items():
    cursor.execute(update_query, user_id)

conn.commit()
df.to_csv("flagged_bots.csv", index=False)

print("Saved filtered flagged bots with reasons.")

