import duckdb
import seaborn as sns

genres_path = "/Users/zzyun/Documents/GitHub/csc369/individualanalysis/genres.csv"
movies_path = "/Users/zzyun/Documents/GitHub/csc369/individualanalysis/movies.csv"
themes_path = "/Users/zzyun/Documents/GitHub/csc369/individualanalysis/themes.csv"

conn = duckdb.connect(database=':memory:')

conn.execute(f"CREATE TABLE genres_df AS SELECT * FROM read_csv('{genres_path}');")
conn.execute(f"CREATE TABLE movies_df AS SELECT * FROM read_csv('{movies_path}');")
conn.execute(f"CREATE TABLE themes_df AS SELECT * FROM read_csv('{themes_path}');")

genres_themes_movies = conn.execute (f"""CREATE TABLE genres_themes_movies AS SELECT movies_df.*, genres_df.*, themes_df.* 
              FROM movies_df 
              INNER JOIN genres_df ON (movies_df.id = genres_df.id)
              INNER JOIN themes_df ON (movies_df.id = themes_df.id)""").fetch_df()

print(conn.execute("""SELECT * FROM genres_themes_movies LIMIT 8""").fetch_df())

print(conn.execute(f"""
            SELECT genre, histogram(minute) AS hist
            FROM genres_themes_movies
            GROUP BY genre
""").fetchall())