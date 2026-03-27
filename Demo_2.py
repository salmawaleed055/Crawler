import requests
import mysql.connector

# Fetch data from API
res = requests.get("https://dummyjson.com/posts")
data = res.json()


conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="2552005Elmarakby$"
)
cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS postdb")

cursor.execute("""CREATE TABLE IF NOT EXISTS postdb.posts (
    id INT PRIMARY KEY,
    title VARCHAR(255),
    body LONGTEXT,
    userId INT
)""")


posts = data.get('posts', [])
posts_filtered = [ (post['id'], post['title'], post['body'], post['userId']) for post in posts if 'title' in post and 'body' in post and 'userId' in post]    
for post in posts:
    try:
        cursor.executemany("""INSERT INTO postdb.posts (id, title, body, userId) 
                         VALUES (%s, %s, %s, %s)""",posts_filtered)
                     
    except Exception as e:
        print(f"Error inserting post {post['id']}: {e}")


conn.commit()
cursor.close()
conn.close()

print(f"Successfully inserted {len(posts)} posts into the database")

