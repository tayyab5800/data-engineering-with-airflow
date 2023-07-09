from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from airflow.models import Variable
from airflow.models.baseoperator import chain
import pandas as pd
from io import StringIO
import json, os

with DAG("iib_part2_dag",
    start_date=datetime(2023, 4 ,1), 
    schedule_interval='@daily', 
    description='Collect movies using a CLI in a Docker container',
    catchup=False) as dag:

    # setting values to airflow
    set_target_year = Variable.set("year", "2012")
    set_genres = Variable.set("genres", "Comedy,Drama")

    # getting values from airflow
    get_target_year = Variable.get('year')
    get_genres = Variable.get('genres')
    
    # Define the first DockerOperator to collect movies   
    step_2_collect_movies = DockerOperator(
        task_id='step_2_collect_movies',
        image='iib_part1',
        container_name='iibflix_filtered_movie',
        api_version='auto',
        command="iibflix filter-by-genre-year-movie-id -f movies.csv -g {} -y {}".format(get_genres, get_target_year),
        xcom_all=True,
        auto_remove=True,
        dag=dag
    )

    # PythonOperator to select only the movieId column
    def select_movie_ids(**context):
        df = pd.read_csv(StringIO(context['ti'].xcom_pull(task_ids='step_2_collect_movies')[0]))
        movie_ids = df["movieId"]
        context['ti'].xcom_push(key='movie_ids', value=movie_ids.to_json(orient='records'))

    step_3_select_movie_ids = PythonOperator(
        task_id='step_3_select_movie_ids',
        python_callable=select_movie_ids,
        provide_context=True,
        dag=dag,
    )

    # DockerOperator to filter users and ratings by movieId(s)
    step_4_1_filter_users_and_ratings = DockerOperator(
        task_id='step_4_1_filter_users_and_ratings',
        image='iib_part1',
        container_name='iibflix_filtered_users_ratings',
        command= 'iibflix filter-ratings -f ratings.csv -m {{ task_instance.xcom_pull(key="movie_ids") }}',
        xcom_all=True,
        auto_remove=True,
        dag=dag
    )

    # DockerOperator to filter users and tags by movieId(s)
    step_4_2_filter_users_and_tags = DockerOperator(
        task_id='step_4_2_filter_users_and_tags',
        image='iib_part1',
        container_name='iibflix_filtered_users_tags',
        command= 'iibflix filter-tags -f tags.csv -m {{ task_instance.xcom_pull(key="movie_ids") }}',
        xcom_all=True,
        auto_remove=True,
        dag=dag
    )

    # PythonOperator to collect sorted list of users with highest value tags on top of the list
    def list_of_users(**context):
        df = pd.read_csv(StringIO(context['ti'].xcom_pull(task_ids='step_4_2_filter_users_and_tags')[0]))
        user_tag_counts = df.groupby(['userId','movieId'])['tag'].count().sort_values(ascending=False).reset_index()

        context['ti'].xcom_push(key='sorted_list_of_users', value=user_tag_counts.to_json(orient='records'))

    step_5_sorted_list_of_users = PythonOperator(
        task_id='step_5_sorted_list_of_users',
        python_callable=list_of_users,
        provide_context=True,
        dag=dag
    )

    # PythonOperator to get the average, maximum and minimum ratings of movies/users
    def avg_max_min(**context):
        df = pd.read_csv(StringIO(context['ti'].xcom_pull(task_ids='step_4_1_filter_users_and_ratings')[0]))
        avg_max_min_per_movie = df.groupby('movieId')['rating'].agg(['mean', 'max', 'min']).reset_index()
        #avg_max_min_per_movie_and_user = df.groupby(['movieId','userId'])['rating'].agg(['mean', 'max', 'min']).reset_index()

        context['ti'].xcom_push(key='avg_max_min_per_movie', value=avg_max_min_per_movie.to_json(orient='records'))
        #context['ti'].xcom_push(key='avg_max_min_per_movie_and_user', value=avg_max_min_per_movie_and_user.to_json(orient='records'))
    
    step_6_avg_max_min = PythonOperator(
        task_id='step_6_avg_max_min',
        python_callable=avg_max_min,
        provide_context=True,
        dag=dag
    )

    # PythonOperator to join the results, exporting CSV and pushing file path to XCOM
    def join_user_rating(**context):
        sorted_list_of_users = pd.DataFrame(json.loads(context['ti'].xcom_pull(key='sorted_list_of_users')))
        avg_max_min_per_movie = pd.DataFrame(json.loads(context['ti'].xcom_pull(key='avg_max_min_per_movie')))

        df = pd.merge(sorted_list_of_users, avg_max_min_per_movie, on='movieId', how='left')

        context['ti'].xcom_push(key='joined_result', value=df.to_json(orient='records'))

        csv_file_path = os.getcwd() + "/{}_{}.csv".format(get_target_year, get_genres)
        df.to_csv(csv_file_path, index=False)

        context['ti'].xcom_push(key='csv_file_path', value=csv_file_path)
    
    step_7_join_user_rating = PythonOperator(
        task_id='step_7_join_user_rating',
        python_callable=join_user_rating,
        provide_context=True,
        dag=dag
    )

    # Setup the sequence of the tasks
    chain(step_2_collect_movies, 
            step_3_select_movie_ids, 
                [step_4_1_filter_users_and_ratings, step_4_2_filter_users_and_tags], 
                    [step_6_avg_max_min, step_5_sorted_list_of_users], 
                        step_7_join_user_rating)
