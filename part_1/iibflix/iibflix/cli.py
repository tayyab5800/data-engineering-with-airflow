import click
import pandas as pd
import ast
  

@click.group()
def iibflix():
    pass

@iibflix.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the input CSV file')
def load(file):
    df = pd.read_csv(file)
    click.echo("{} has {} records. Only First 5 rows are shown below".format(file, len(df)))
    click.echo(df.head())

@iibflix.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the input CSV file')
@click.option('-i', '--index_col', type=str, help='Name or index of column to use as the row labels')
def retrieve(file, index_col):
    df = pd.read_csv(file, index_col=index_col)
    click.echo(df.head())

@iibflix.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the input CSV file')
@click.option('-m', '--movie_id', required=True, help='Movie ID to filter by')
@click.option('-o', '--output', type=click.Choice(['json', 'csv']), default='csv', help='Output format (csv or json)')
def filter_tags(file, movie_id, output):
    chunk_size = 100000
    df_filtered_list = []
    for df in pd.read_csv(file, chunksize=chunk_size):
        movie_id_list = ast.literal_eval(movie_id)
        df_filtered = df[df['movieId'].isin(movie_id_list)][['movieId', 'userId', 'tag']]

        df_filtered_list.append(df_filtered)

    df_filtered = pd.concat(df_filtered_list)    
    if output == 'json':
        df_filtered = df_filtered.to_json(index=False)
        click.echo(df_filtered)
    else:
        df_filtered = df_filtered.to_csv(index=False)
        click.echo(df_filtered)

@iibflix.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the input CSV file')
@click.option('-u', '--user_id', help='User ID to filter by')
@click.option('-m', '--movie_id', help='Movie ID to filter by')
@click.option('--output', type=click.Choice(['json', 'csv']), default='csv', help='Output format (csv or json)')
def filter_ratings(file, user_id, movie_id, output):
    # ratings.csv has 25000095 records. For optimization and 
    # using less recourses we are reading and processing data in chunks
    chunk_size = 100000
    df_filtered_list = []
    for df in pd.read_csv(file, chunksize=chunk_size):
        if user_id and movie_id:
            user_id_list = ast.literal_eval(user_id)
            movie_id_list = ast.literal_eval(movie_id)
            df_filtered = df[(df['userId'].isin(user_id_list)) & (df['movieId'].isin(movie_id_list))][['userId', 'movieId', 'rating']]
        else:               
            if user_id:
                user_id_list = ast.literal_eval(user_id)
                df_filtered = df[df['userId'].isin(user_id_list)][['userId', 'movieId', 'rating']]
            elif movie_id:
                movie_id_list = ast.literal_eval(movie_id)
                df_filtered = df[df['movieId'].isin(movie_id_list)][['userId', 'movieId', 'rating']]
            else:
                click.echo('Please provide at least one of --user_id or --movie_id')

        df_filtered_list.append(df_filtered)

    df_filtered = pd.concat(df_filtered_list)    
    if output == 'json':
        df_filtered = df_filtered.to_json(index=False)
        click.echo(df_filtered)
    else:
        df_filtered = df_filtered.to_csv(index=False)
        click.echo(df_filtered)

@iibflix.command()
@click.option('-f', '--file', type=click.Path(exists=True), required=True, help='Path to the input CSV file')
@click.option('-g', '--genre', help='Genre to filter on')
@click.option('-y', '--year', help='Year to filter on')
@click.option('-m', '--movie_id', help='Movie ID to filter on')
def filter_by_genre_year_movie_id(file, genre, year, movie_id):
    df = pd.read_csv(file)
    if genre:
        if "," in genre:
            genre_list = genre.split(',')
        else:
            genre_list = genre.split()
        all_temp_all = pd.DataFrame()
        temp_genre_df = pd.DataFrame()
        for i in genre_list:
            temp_genre_df = df[df['genres'].str.contains(i.title() , na=False)]
            all_temp_all = pd.concat([all_temp_all ,temp_genre_df])
        all_temp_all = all_temp_all.drop_duplicates()
        df = all_temp_all
    if year:
        year_list = ast.literal_eval(year)
        if isinstance(year_list, int):
            year_list = [year_list]
        all_temp_all = pd.DataFrame()
        for year in year_list:
            temp_year_df = df[df['title'].str.endswith(f"({year})")]
            all_temp_all = pd.concat([all_temp_all ,temp_year_df])
        all_temp_all = all_temp_all.drop_duplicates()
        df = all_temp_all
    if movie_id:
        movie_id_list = ast.literal_eval(movie_id)
        df = df[df['movieId'].isin(movie_id_list)]

    df = df.to_csv(index=False)
    click.echo(df)
